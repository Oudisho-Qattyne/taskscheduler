"""Task Scheduler - Main Application."""
import os
import sys
import json
import tempfile
import subprocess
import traceback
import hashlib
import base64
import math
import re
import random
import urllib
import itertools
import collections
from datetime import datetime, timedelta
from collections import deque
from functools import wraps

from flask import (
    Flask, render_template, request, redirect, url_for, flash,
    session, jsonify, abort, Response, send_file
)
from werkzeug.utils import secure_filename
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

from models import db, User, Task, Flow, Function, Constant, ExecutionLog, AuditLog

# Configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'scheduler.db')
SCRIPTS_DIR = os.path.join(BASE_DIR, 'scripts')
os.makedirs(SCRIPTS_DIR, exist_ok=True)

app = Flask(__name__)
SECRET_KEY_FILE = os.path.join(BASE_DIR, '.secret_key')
if os.path.exists(SECRET_KEY_FILE):
    with open(SECRET_KEY_FILE, 'rb') as f:
        SECRET_KEY = f.read()
else:
    SECRET_KEY = os.urandom(32)
    with open(SECRET_KEY_FILE, 'wb') as f:
        f.write(SECRET_KEY)
app.config['SECRET_KEY'] = SECRET_KEY
app.config['SQLALCHEMY_DATABASE_URI'] = f'sqlite:///{DB_PATH}'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024

db.init_app(app)

# APScheduler setup
scheduler = BackgroundScheduler(
    jobstores={
        'default': {'type': 'sqlalchemy', 'url': f'sqlite:///{DB_PATH}', 'tablename': 'apscheduler_jobs'}
    },
    executors={
        'default': {'type': 'threadpool', 'max_workers': 10}
    },
    job_defaults={
        'coalesce': True,
        'max_instances': 1,
        'misfire_grace_time': 3600
    }
)

# Safe builtins for function execution
SAFE_BUILTINS = {
    'print': print,
    'len': len,
    'range': range,
    'str': str,
    'int': int,
    'float': float,
    'bool': bool,
    'list': list,
    'dict': dict,
    'json': json,
    'datetime': __import__('datetime'),
    'os': os,
    'sys': sys,
    'math': math,
    're': re,
    'random': random,
    'urllib': urllib,
    'base64': base64,
    'hashlib': hashlib,
    'itertools': itertools,
    'collections': collections,
    'type': type,
    'isinstance': isinstance,
    'hasattr': hasattr,
    'getattr': getattr,
    'setattr': setattr,
    'Exception': Exception,
    'ValueError': ValueError,
    'KeyError': KeyError,
    'IndexError': IndexError,
    'AttributeError': AttributeError,
    'RuntimeError': RuntimeError,
    'True': True,
    'False': False,
    'None': None,
}


def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            if request.is_json or request.path.startswith('/api/'):
                return jsonify({'error': 'Unauthorized'}), 401
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function


def log_audit(action, entity_type, entity_id=None, details=None):
    """Log an audit entry."""
    username = session.get('username', 'system')
    entry = AuditLog(
        username=username,
        action=action,
        entity_type=entity_type,
        entity_id=entity_id,
        details=json.dumps(details) if details else None
    )
    db.session.add(entry)
    db.session.commit()


def build_trigger(schedule_type, config):
    """Build an APScheduler trigger from schedule config."""
    if schedule_type == 'once':
        run_date = config.get('datetime')
        if run_date:
            return DateTrigger(run_date=datetime.fromisoformat(run_date))
    elif schedule_type == 'interval':
        value = int(config.get('value', 1))
        unit = config.get('unit', 'minutes')
        kwargs = {}
        if unit == 'minutes':
            kwargs['minutes'] = value
        elif unit == 'hours':
            kwargs['hours'] = value
        elif unit == 'days':
            kwargs['days'] = value
        return IntervalTrigger(**kwargs)
    elif schedule_type == 'daily':
        time_str = config.get('time', '00:00')
        hour, minute = map(int, time_str.split(':'))
        return CronTrigger(hour=hour, minute=minute)
    elif schedule_type == 'weekly':
        day = config.get('day', 'mon')
        time_str = config.get('time', '00:00')
        hour, minute = map(int, time_str.split(':'))
        day_map = {'mon': 0, 'tue': 1, 'wed': 2, 'thu': 3, 'fri': 4, 'sat': 5, 'sun': 6}
        return CronTrigger(day_of_week=day_map.get(day, 0), hour=hour, minute=minute)
    elif schedule_type == 'monthly':
        day = int(config.get('day', 1))
        time_str = config.get('time', '00:00')
        hour, minute = map(int, time_str.split(':'))
        return CronTrigger(day=day, hour=hour, minute=minute)
    elif schedule_type == 'monthly_weekday':
        ordinal = config.get('ordinal', '1st')
        weekday = config.get('weekday', 'mon')
        time_str = config.get('time', '00:00')
        hour, minute = map(int, time_str.split(':'))
        day_map = {'mon': 'mon', 'tue': 'tue', 'wed': 'wed', 'thu': 'thu', 'fri': 'fri', 'sat': 'sat', 'sun': 'sun'}
        day_str = f"{ordinal} {day_map.get(weekday, 'mon')}"
        return CronTrigger(day=day_str, hour=hour, minute=minute)
    return None


def get_script_path(task):
    """Resolve script path - relative to scripts dir or absolute."""
    if not task.script_path:
        return None
    if os.path.isabs(task.script_path):
        return task.script_path if os.path.exists(task.script_path) else None
    # Relative path - look in scripts dir
    rel_path = os.path.join(SCRIPTS_DIR, task.script_path)
    if os.path.exists(rel_path):
        return rel_path
    # Try with just the filename
    fname = os.path.basename(task.script_path)
    alt_path = os.path.join(SCRIPTS_DIR, fname)
    if os.path.exists(alt_path):
        return alt_path
    return None


def save_script_code(name, code):
    """Save code to scripts directory and return relative path."""
    safe_name = secure_filename(name.replace(' ', '_').lower())
    if not safe_name.endswith('.py'):
        safe_name += '.py'
    path = os.path.join(SCRIPTS_DIR, safe_name)
    with open(path, 'w') as f:
        f.write(code)
    return safe_name


def execute_task_script(task_id):
    """Execute a script task."""
    with app.app_context():
        task = Task.query.get(task_id)
        if not task:
            return

        start_time = datetime.utcnow()
        output_dir = os.path.join(BASE_DIR, 'outputs', f'task_{task_id}')
        os.makedirs(output_dir, exist_ok=True)

        env = os.environ.copy()
        env.update(task.get_env_vars())
        env['TASK_OUTPUT_DIR'] = output_dir
        env['TASK_ID'] = str(task.id)
        env['TASK_NAME'] = task.name

        script_path = get_script_path(task)

        attempt = 1
        max_retries = task.max_retries or 0
        status = 'failed'
        output_text = ''

        while attempt <= max_retries + 1:
            try:
                if script_path and os.path.exists(script_path):
                    cmd = [sys.executable, script_path] + (task.cli_args or '').split()
                    proc = subprocess.run(
                        cmd,
                        capture_output=True,
                        text=True,
                        timeout=task.timeout_seconds or 30,
                        env=env,
                        cwd=BASE_DIR
                    )
                    output_text = proc.stdout + ('\n' + proc.stderr if proc.stderr else '')
                    if proc.returncode == 0:
                        status = 'success'
                        break
                    else:
                        status = 'failed'
                else:
                    output_text = f"Script not found: {task.script_path}"
                    status = 'failed'
                    break
            except subprocess.TimeoutExpired:
                output_text = f"Task timed out after {task.timeout_seconds}s"
                status = 'failed'
            except Exception as e:
                output_text = f"Execution error: {str(e)}\n{traceback.format_exc()}"
                status = 'failed'

            attempt += 1
            if attempt <= max_retries + 1:
                import time
                time.sleep(2 ** (attempt - 1))

        duration = int((datetime.utcnow() - start_time).total_seconds() * 1000)

        task.last_run_time = start_time
        task.last_run_status = status
        task.last_run_output = output_text[:10000]
        db.session.commit()

        log = ExecutionLog(
            task_id=task.id,
            run_time=start_time,
            status=status,
            output=output_text[:10000],
            duration_ms=duration,
            attempt_number=attempt
        )
        db.session.add(log)
        db.session.commit()


def execute_flow_graph(flow_id, test_mode=False, test_nodes=None, test_connections=None):
    """Execute a flow graph with topological sorting."""
    with app.app_context():
        flow = Flow.query.get(flow_id)
        if not flow:
            return {'success': False, 'error': 'Flow not found'}

        nodes = test_nodes if test_mode else flow.get_nodes()
        connections = test_connections if test_mode else flow.get_connections()

        if not nodes:
            return {'success': True, 'output': 'No nodes to execute', 'node_errors': {}}

        # Build adjacency list and in-degree count
        node_map = {n['id']: n for n in nodes}
        in_degree = {n['id']: 0 for n in nodes}
        adj = {n['id']: [] for n in nodes}

        for conn in connections:
            src = conn.get('source')
            tgt = conn.get('target')
            if src in node_map and tgt in node_map:
                adj[src].append(conn)
                in_degree[tgt] += 1

        # Topological sort
        queue = deque([nid for nid, deg in in_degree.items() if deg == 0])
        execution_order = []

        while queue:
            nid = queue.popleft()
            execution_order.append(nid)
            for conn in adj.get(nid, []):
                tgt = conn['target']
                in_degree[tgt] -= 1
                if in_degree[tgt] == 0:
                    queue.append(tgt)

        # Check for cycles
        if len(execution_order) != len(nodes):
            return {'success': False, 'error': 'Cycle detected in flow graph', 'node_errors': {}}

        # Execute nodes
        node_outputs = {}
        node_errors = {}
        overall_output = []

        for nid in execution_order:
            node = node_map[nid]
            node_type = node.get('type', 'function')

            try:
                if node_type == 'constant':
                    const_id = node.get('constant_id')
                    const = Constant.query.get(const_id)
                    if const:
                        val = const.value
                        if const.type == 'int':
                            val = int(val)
                        elif const.type == 'float':
                            val = float(val)
                        elif const.type == 'bool':
                            val = val.lower() in ('true', '1', 'yes', 'on')
                        elif const.type == 'json':
                            val = json.loads(val)
                        node_outputs[nid] = val
                    else:
                        node_outputs[nid] = None

                elif node_type == 'function':
                    func_id = node.get('function_id')
                    func = Function.query.get(func_id)
                    if not func:
                        raise ValueError(f"Function {func_id} not found")

                    # Gather kwargs
                    kwargs = {}

                    # From incoming connections
                    for conn in connections:
                        if conn['target'] == nid:
                            src_id = conn['source']
                            param_name = conn.get('targetParam', 'input')
                            if src_id in node_outputs:
                                kwargs[param_name] = node_outputs[src_id]

                    # From hardcoded inputs
                    for param in func.get_params():
                        pname = param['name']
                        if pname not in kwargs and pname in node.get('inputs', {}):
                            val = node['inputs'][pname]
                            ptype = param.get('type', 'str')
                            if ptype == 'int' and val:
                                val = int(val)
                            elif ptype == 'float' and val:
                                val = float(val)
                            elif ptype == 'bool' and val:
                                val = val.lower() in ('true', '1', 'yes', 'on')
                            elif ptype == 'json' and val:
                                val = json.loads(val)
                            kwargs[pname] = val

                    # Execute function in restricted namespace
                    namespace = SAFE_BUILTINS.copy()
                    exec(func.code, namespace)
                    func_obj = namespace.get(func.name)
                    if not func_obj:
                        raise ValueError(f"Function {func.name} not defined in code")

                    result = func_obj(**kwargs)
                    node_outputs[nid] = result
                    overall_output.append(f"{func.name}: {result}")

            except Exception as e:
                error_msg = f"{type(e).__name__}: {str(e)}"
                node_errors[nid] = error_msg
                node_outputs[nid] = None
                overall_output.append(f"{node.get('name', nid)}: ERROR - {error_msg}")

        output_text = '\n'.join(overall_output) if overall_output else 'Flow executed successfully'

        if not test_mode:
            start_time = datetime.utcnow()
            status = 'success' if not node_errors else 'failed'
            duration = 0

            flow.last_run_time = start_time
            flow.last_run_status = status
            flow.last_run_output = output_text[:10000]
            flow.set_node_errors(node_errors)
            db.session.commit()

            log = ExecutionLog(
                flow_id=flow.id,
                run_time=start_time,
                status=status,
                output=output_text[:10000],
                duration_ms=duration,
                attempt_number=1
            )
            db.session.add(log)
            db.session.commit()

        return {
            'success': True,
            'output': output_text,
            'node_errors': node_errors,
            'node_outputs': {k: str(v) for k, v in node_outputs.items()}
        }


def schedule_task(task):
    """Schedule a task in APScheduler."""
    job_id = f"task_{task.id}"
    try:
        scheduler.remove_job(job_id)
    except:
        pass

    if not task.enabled:
        return

    trigger = build_trigger(task.schedule_type, task.get_schedule_config())
    if trigger:
        scheduler.add_job(
            execute_task_script,
            trigger=trigger,
            id=job_id,
            args=[task.id],
            max_instances=task.max_instances or 1,
            replace_existing=True
        )
        try:
            job = scheduler.get_job(job_id)
            if job and job.next_run_time:
                task.next_run_time = job.next_run_time
                db.session.commit()
        except:
            pass


def schedule_flow(flow):
    """Schedule a flow in APScheduler."""
    job_id = f"flow_{flow.id}"
    try:
        scheduler.remove_job(job_id)
    except:
        pass

    if not flow.enabled:
        return

    trigger = build_trigger(flow.schedule_type, flow.get_schedule_config())
    if trigger:
        scheduler.add_job(
            execute_flow_graph,
            trigger=trigger,
            id=job_id,
            args=[flow.id],
            replace_existing=True
        )
        try:
            job = scheduler.get_job(job_id)
            if job and job.next_run_time:
                flow.next_run_time = job.next_run_time
                db.session.commit()
        except:
            pass


def reload_schedules():
    """Reload all schedules from database."""
    with app.app_context():
        for task in Task.query.all():
            schedule_task(task)
        for flow in Flow.query.all():
            schedule_flow(flow)


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        user = User.query.filter_by(username=username).first()
        if user and user.check_password(password):
            session['user_id'] = user.id
            session['username'] = user.username
            log_audit('login', 'user', user.id)
            return redirect(url_for('dashboard'))
        flash('Invalid username or password', 'error')
    return render_template('login.html')


@app.route('/logout')
def logout():
    log_audit('logout', 'user', session.get('user_id'))
    session.clear()
    return redirect(url_for('login'))


@app.route('/')
@login_required
def dashboard():
    total_items = Task.query.count() + Flow.query.count()
    enabled_items = Task.query.filter_by(enabled=True).count() + Flow.query.filter_by(enabled=True).count()

    last_24h = datetime.utcnow() - timedelta(hours=24)
    recent_logs = ExecutionLog.query.filter(ExecutionLog.run_time >= last_24h).all()
    success_count = sum(1 for l in recent_logs if l.status == 'success')
    total_recent = len(recent_logs)
    success_rate = round((success_count / total_recent * 100), 1) if total_recent > 0 else 0

    avg_duration = 0
    if recent_logs:
        durations = [l.duration_ms for l in recent_logs if l.duration_ms]
        avg_duration = round(sum(durations) / len(durations), 0) if durations else 0

    upcoming_tasks = Task.query.filter(Task.next_run_time >= datetime.utcnow()).order_by(Task.next_run_time).limit(5).all()
    upcoming_flows = Flow.query.filter(Flow.next_run_time >= datetime.utcnow()).order_by(Flow.next_run_time).limit(5).all()
    all_tasks = Task.query.order_by(Task.created_at.desc()).all()
    all_flows = Flow.query.order_by(Flow.created_at.desc()).all()

    # Calendar data - all scheduled items
    calendar_tasks = Task.query.filter(Task.enabled == True, Task.next_run_time != None).all()
    calendar_flows = Flow.query.filter(Flow.enabled == True, Flow.next_run_time != None).all()

    return render_template('dashboard.html',
        total_items=total_items,
        enabled_items=enabled_items,
        success_rate=success_rate,
        executions_24h=total_recent,
        avg_duration=int(avg_duration),
        upcoming_tasks=upcoming_tasks,
        upcoming_flows=upcoming_flows,
        tasks=all_tasks,
        flows=all_flows,
        calendar_tasks=calendar_tasks,
        calendar_flows=calendar_flows
    )


@app.route('/tasks')
@login_required
def tasks_list():
    search = request.args.get('search', '')
    query = Task.query
    if search:
        query = query.filter(Task.name.contains(search))
    tasks = query.order_by(Task.created_at.desc()).all()
    return render_template('tasks.html', tasks=tasks, search=search)


@app.route('/scripts')
@login_required
def list_scripts():
    """List available scripts in the scripts directory."""
    scripts = []
    if os.path.exists(SCRIPTS_DIR):
        for f in sorted(os.listdir(SCRIPTS_DIR)):
            if f.endswith('.py'):
                scripts.append(f)
    return jsonify(scripts)


@app.route('/task/new', methods=['GET', 'POST'])
@login_required
def task_new():
    if request.method == 'POST':
        task = Task(name=request.form['name'])
        task.schedule_type = request.form.get('schedule_type', 'once')
        task.set_schedule_config(json.loads(request.form.get('schedule_config', '{}')))
        task.enabled = request.form.get('enabled') == 'on'
        task.timeout_seconds = int(request.form.get('timeout_seconds', 30))
        task.max_retries = int(request.form.get('max_retries', 0))
        task.max_instances = int(request.form.get('max_instances', 1))
        task.cli_args = request.form.get('cli_args', '')
        task.tags = request.form.get('tags', '')

        # Handle script source
        source_type = request.form.get('source_type', 'code')
        if source_type == 'code':
            code = request.form.get('script_code', '')
            if code.strip():
                rel_path = save_script_code(request.form['name'], code)
                task.script_path = rel_path
            else:
                task.script_path = ''
        else:
            task.script_path = request.form.get('existing_script', '')

        env_vars = {}
        for key, val in zip(request.form.getlist('env_key[]'), request.form.getlist('env_val[]')):
            if key:
                env_vars[key] = val
        task.set_env_vars(env_vars)

        db.session.add(task)
        db.session.commit()
        schedule_task(task)
        log_audit('create', 'task', task.id)
        flash('Task created successfully', 'success')
        return redirect(url_for('tasks_list'))

    return render_template('task_form.html', task=None)


@app.route('/task/<int:id>/edit', methods=['GET', 'POST'])
@login_required
def task_edit(id):
    task = Task.query.get_or_404(id)
    if request.method == 'POST':
        task.name = request.form['name']
        task.schedule_type = request.form.get('schedule_type', 'once')
        task.set_schedule_config(json.loads(request.form.get('schedule_config', '{}')))
        task.enabled = request.form.get('enabled') == 'on'
        task.timeout_seconds = int(request.form.get('timeout_seconds', 30))
        task.max_retries = int(request.form.get('max_retries', 0))
        task.max_instances = int(request.form.get('max_instances', 1))
        task.cli_args = request.form.get('cli_args', '')
        task.tags = request.form.get('tags', '')

        # Handle script source
        source_type = request.form.get('source_type', 'code')
        if source_type == 'code':
            code = request.form.get('script_code', '')
            if code.strip():
                rel_path = save_script_code(request.form['name'], code)
                task.script_path = rel_path
            else:
                task.script_path = ''
        else:
            task.script_path = request.form.get('existing_script', '')

        env_vars = {}
        for key, val in zip(request.form.getlist('env_key[]'), request.form.getlist('env_val[]')):
            if key:
                env_vars[key] = val
        task.set_env_vars(env_vars)

        db.session.commit()
        schedule_task(task)
        log_audit('update', 'task', task.id)
        flash('Task updated successfully', 'success')
        return redirect(url_for('tasks_list'))

    # Load script code if it exists
    script_code = ''
    if task.script_path:
        full_path = get_script_path(task)
        if full_path and os.path.exists(full_path):
            try:
                with open(full_path, 'r') as f:
                    script_code = f.read()
            except:
                pass

    return render_template('task_form.html', task=task, script_code=script_code)


@app.route('/task/<int:id>')
@login_required
def task_detail(id):
    task = Task.query.get_or_404(id)
    logs = ExecutionLog.query.filter_by(task_id=id).order_by(ExecutionLog.run_time.desc()).limit(20).all()
    return render_template('task_detail.html', task=task, logs=logs)


@app.route('/task/<int:id>/delete', methods=['POST'])
@login_required
def task_delete(id):
    task = Task.query.get_or_404(id)
    try:
        scheduler.remove_job(f"task_{task.id}")
    except:
        pass
    db.session.delete(task)
    db.session.commit()
    log_audit('delete', 'task', id)
    flash('Task deleted', 'success')
    return redirect(url_for('tasks_list'))


@app.route('/task/<int:id>/run', methods=['POST'])
@login_required
def task_run(id):
    task = Task.query.get_or_404(id)
    execute_task_script(task.id)
    log_audit('run', 'task', task.id)
    flash('Task executed', 'success')
    return redirect(url_for('tasks_list'))


@app.route('/task/<int:id>/toggle', methods=['POST'])
@login_required
def task_toggle(id):
    task = Task.query.get_or_404(id)
    task.enabled = not task.enabled
    db.session.commit()
    schedule_task(task)
    action = 'enable' if task.enabled else 'disable'
    log_audit(action, 'task', task.id)
    return jsonify({'enabled': task.enabled})


@app.route('/flows')
@login_required
def flows_list():
    search = request.args.get('search', '')
    query = Flow.query
    if search:
        query = query.filter(Flow.name.contains(search))
    flows = query.order_by(Flow.created_at.desc()).all()
    return render_template('flows.html', flows=flows, search=search)


@app.route('/flow/new', methods=['GET', 'POST'])
@login_required
def flow_new():
    if request.method == 'POST':
        data = request.get_json()
        flow = Flow(name=data['name'])
        flow.schedule_type = data.get('schedule_type', 'once')
        flow.set_schedule_config(data.get('schedule_config', {}))
        flow.enabled = data.get('enabled', True)
        flow.set_nodes(data.get('nodes', []))
        flow.set_connections(data.get('connections', []))
        flow.timeout_seconds = data.get('timeout_seconds', 30)
        flow.max_retries = data.get('max_retries', 0)

        db.session.add(flow)
        db.session.commit()
        schedule_flow(flow)
        log_audit('create', 'flow', flow.id)
        return jsonify({'id': flow.id, 'success': True})

    functions = Function.query.all()
    constants = Constant.query.all()
    return render_template('flow_editor.html', flow=None, functions=functions, constants=constants)


@app.route('/flow/<int:id>/edit', methods=['GET', 'POST'])
@login_required
def flow_edit(id):
    flow = Flow.query.get_or_404(id)
    if request.method == 'POST':
        data = request.get_json()
        flow.name = data['name']
        flow.schedule_type = data.get('schedule_type', 'once')
        flow.set_schedule_config(data.get('schedule_config', {}))
        flow.enabled = data.get('enabled', True)
        flow.set_nodes(data.get('nodes', []))
        flow.set_connections(data.get('connections', []))
        flow.timeout_seconds = data.get('timeout_seconds', 30)
        flow.max_retries = data.get('max_retries', 0)

        db.session.commit()
        schedule_flow(flow)
        log_audit('update', 'flow', flow.id)
        return jsonify({'success': True})

    functions = Function.query.all()
    constants = Constant.query.all()
    return render_template('flow_editor.html', flow=flow, functions=functions, constants=constants)


@app.route('/flow/<int:id>')
@login_required
def flow_detail(id):
    flow = Flow.query.get_or_404(id)
    logs = ExecutionLog.query.filter_by(flow_id=id).order_by(ExecutionLog.run_time.desc()).limit(20).all()
    return render_template('flow_detail.html', flow=flow, logs=logs)


@app.route('/flow/<int:id>/delete', methods=['POST'])
@login_required
def flow_delete(id):
    flow = Flow.query.get_or_404(id)
    try:
        scheduler.remove_job(f"flow_{flow.id}")
    except:
        pass
    db.session.delete(flow)
    db.session.commit()
    log_audit('delete', 'flow', id)
    flash('Flow deleted', 'success')
    return redirect(url_for('flows_list'))


@app.route('/flow/<int:id>/run', methods=['POST'])
@login_required
def flow_run(id):
    flow = Flow.query.get_or_404(id)
    result = execute_flow_graph(flow.id)
    log_audit('run', 'flow', flow.id)
    return jsonify(result)


@app.route('/flow/<int:id>/test', methods=['POST'])
@login_required
def flow_test(id):
    data = request.get_json()
    result = execute_flow_graph(
        id,
        test_mode=True,
        test_nodes=data.get('nodes', []),
        test_connections=data.get('connections', [])
    )
    return jsonify(result)


@app.route('/flow/<int:id>/toggle', methods=['POST'])
@login_required
def flow_toggle(id):
    flow = Flow.query.get_or_404(id)
    flow.enabled = not flow.enabled
    db.session.commit()
    schedule_flow(flow)
    action = 'enable' if flow.enabled else 'disable'
    log_audit(action, 'flow', flow.id)
    return jsonify({'enabled': flow.enabled})


@app.route('/functions', methods=['GET', 'POST'])
@login_required
def functions_list():
    if request.method == 'POST':
        data = request.get_json()
        func = Function(
            name=data['name'],
            description=data.get('description', ''),
            code=data['code'],
            return_type=data.get('return_type', 'any')
        )
        func.set_params(data.get('params', []))
        db.session.add(func)
        db.session.commit()
        log_audit('create', 'function', func.id)
        return jsonify({'id': func.id, 'success': True})

    funcs = Function.query.order_by(Function.name).all()
    return jsonify([{
        'id': f.id,
        'name': f.name,
        'description': f.description,
        'code': f.code,
        'params': f.get_params(),
        'return_type': f.return_type
    } for f in funcs])


@app.route('/functions/<int:id>', methods=['GET', 'PUT', 'DELETE'])
@login_required
def function_detail(id):
    func = Function.query.get_or_404(id)
    if request.method == 'GET':
        return jsonify({
            'id': func.id,
            'name': func.name,
            'description': func.description,
            'code': func.code,
            'params': func.get_params(),
            'return_type': func.return_type
        })
    elif request.method == 'PUT':
        data = request.get_json()
        func.name = data['name']
        func.description = data.get('description', '')
        func.code = data['code']
        func.set_params(data.get('params', []))
        func.return_type = data.get('return_type', 'any')
        db.session.commit()
        log_audit('update', 'function', func.id)
        return jsonify({'success': True})
    elif request.method == 'DELETE':
        db.session.delete(func)
        db.session.commit()
        log_audit('delete', 'function', id)
        return jsonify({'success': True})


@app.route('/constants', methods=['GET', 'POST'])
@login_required
def constants_list():
    if request.method == 'POST':
        data = request.get_json()
        const = Constant(
            name=data['name'],
            value=str(data['value']),
            type=data.get('type', 'str')
        )
        db.session.add(const)
        db.session.commit()
        log_audit('create', 'constant', const.id)
        return jsonify({'id': const.id, 'success': True})

    consts = Constant.query.order_by(Constant.name).all()
    return jsonify([{
        'id': c.id,
        'name': c.name,
        'value': c.value,
        'type': c.type
    } for c in consts])


@app.route('/constants/<int:id>', methods=['GET', 'PUT', 'DELETE'])
@login_required
def constant_detail(id):
    const = Constant.query.get_or_404(id)
    if request.method == 'GET':
        return jsonify({
            'id': const.id,
            'name': const.name,
            'value': const.value,
            'type': const.type
        })
    elif request.method == 'PUT':
        data = request.get_json()
        const.name = data['name']
        const.value = str(data['value'])
        const.type = data.get('type', 'str')
        db.session.commit()
        log_audit('update', 'constant', const.id)
        return jsonify({'success': True})
    elif request.method == 'DELETE':
        db.session.delete(const)
        db.session.commit()
        log_audit('delete', 'constant', id)
        return jsonify({'success': True})


@app.route('/test-code', methods=['POST'])
@login_required
def test_code():
    """Test run code in a temporary file."""
    data = request.get_json()
    code = data.get('code', '')
    filename = data.get('filename', 'test_script.py')

    try:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False, dir=BASE_DIR) as f:
            f.write(code)
            temp_path = f.name

        env = os.environ.copy()
        env['TASK_OUTPUT_DIR'] = BASE_DIR
        env['TASK_ID'] = '0'
        env['TASK_NAME'] = 'Test'

        proc = subprocess.run(
            [sys.executable, temp_path],
            capture_output=True,
            text=True,
            timeout=30,
            env=env,
            cwd=BASE_DIR
        )

        os.unlink(temp_path)

        return jsonify({
            'success': proc.returncode == 0,
            'stdout': proc.stdout,
            'stderr': proc.stderr,
            'returncode': proc.returncode
        })
    except subprocess.TimeoutExpired:
        return jsonify({'success': False, 'error': 'Code execution timed out after 30s'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/audit-log')
@login_required
def audit_log():
    page = request.args.get('page', 1, type=int)
    per_page = 50
    logs = AuditLog.query.order_by(AuditLog.timestamp.desc()).paginate(
        page=page, per_page=per_page, error_out=False
    )
    return render_template('audit_log.html', logs=logs)


@app.route('/timeline-partial')
@login_required
def timeline_partial():
    """Return timeline HTML partial for AJAX refresh."""
    now = datetime.utcnow()
    upcoming_tasks = Task.query.filter(
        Task.enabled == True,
        Task.next_run_time >= now
    ).order_by(Task.next_run_time).all()

    upcoming_flows = Flow.query.filter(
        Flow.enabled == True,
        Flow.next_run_time >= now
    ).order_by(Flow.next_run_time).all()

    items = []
    for t in upcoming_tasks:
        items.append({'type': 'task', 'obj': t, 'time': t.next_run_time})
    for f in upcoming_flows:
        items.append({'type': 'flow', 'obj': f, 'time': f.next_run_time})

    items.sort(key=lambda x: x['time'] if x['time'] else datetime.max)

    return render_template('timeline_partial.html', items=items, now=now)


@app.route('/export')
@login_required
def export_data():
    """Export all data as JSON."""
    data = {
        'tasks': [],
        'flows': [],
        'functions': [],
        'constants': [],
        'exported_at': datetime.utcnow().isoformat()
    }

    for t in Task.query.all():
        data['tasks'].append({
            'name': t.name,
            'script_path': t.script_path,
            'schedule_type': t.schedule_type,
            'schedule_config': t.get_schedule_config(),
            'enabled': False,
            'timeout_seconds': t.timeout_seconds,
            'env_vars': t.get_env_vars(),
            'cli_args': t.cli_args,
            'tags': t.tags,
            'max_retries': t.max_retries,
            'max_instances': t.max_instances
        })

    for f in Flow.query.all():
        data['flows'].append({
            'name': f.name,
            'schedule_type': f.schedule_type,
            'schedule_config': f.get_schedule_config(),
            'enabled': False,
            'nodes': f.get_nodes(),
            'connections': f.get_connections(),
            'timeout_seconds': f.timeout_seconds,
            'max_retries': f.max_retries
        })

    for fn in Function.query.all():
        data['functions'].append({
            'name': fn.name,
            'description': fn.description,
            'code': fn.code,
            'params': fn.get_params(),
            'return_type': fn.return_type
        })

    for c in Constant.query.all():
        data['constants'].append({
            'name': c.name,
            'value': c.value,
            'type': c.type
        })

    filename = f"scheduler_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    return Response(
        json.dumps(data, indent=2),
        mimetype='application/json',
        headers={'Content-Disposition': f'attachment; filename={filename}'}
    )


@app.route('/import', methods=['POST'])
@login_required
def import_data():
    """Import data from JSON."""
    file = request.files.get('file')
    if not file:
        flash('No file provided', 'error')
        return redirect(url_for('dashboard'))

    try:
        data = json.load(file)

        for t_data in data.get('tasks', []):
            task = Task(name=t_data['name'])
            task.script_path = t_data.get('script_path', '')
            task.schedule_type = t_data.get('schedule_type', 'once')
            task.set_schedule_config(t_data.get('schedule_config', {}))
            task.enabled = False
            task.timeout_seconds = t_data.get('timeout_seconds', 30)
            task.set_env_vars(t_data.get('env_vars', {}))
            task.cli_args = t_data.get('cli_args', '')
            task.tags = t_data.get('tags', '')
            task.max_retries = t_data.get('max_retries', 0)
            task.max_instances = t_data.get('max_instances', 1)
            db.session.add(task)

        for f_data in data.get('flows', []):
            flow = Flow(name=f_data['name'])
            flow.schedule_type = f_data.get('schedule_type', 'once')
            flow.set_schedule_config(f_data.get('schedule_config', {}))
            flow.enabled = False
            flow.set_nodes(f_data.get('nodes', []))
            flow.set_connections(f_data.get('connections', []))
            flow.timeout_seconds = f_data.get('timeout_seconds', 30)
            flow.max_retries = f_data.get('max_retries', 0)
            db.session.add(flow)

        for fn_data in data.get('functions', []):
            func = Function(
                name=fn_data['name'],
                description=fn_data.get('description', ''),
                code=fn_data['code'],
                return_type=fn_data.get('return_type', 'any')
            )
            func.set_params(fn_data.get('params', []))
            db.session.add(func)

        for c_data in data.get('constants', []):
            const = Constant(
                name=c_data['name'],
                value=str(c_data['value']),
                type=c_data.get('type', 'str')
            )
            db.session.add(const)

        db.session.commit()
        reload_schedules()
        log_audit('import', 'system')
        flash('Data imported successfully. All items disabled by default.', 'success')
    except Exception as e:
        flash(f'Import failed: {str(e)}', 'error')

    return redirect(url_for('dashboard'))


# REST API
@app.route('/api/v1/tasks')
@login_required
def api_tasks():
    tasks = Task.query.all()
    return jsonify([{
        'id': t.id,
        'name': t.name,
        'schedule_type': t.schedule_type,
        'enabled': t.enabled,
        'last_run_status': t.last_run_status,
        'next_run_time': t.next_run_time.isoformat() if t.next_run_time else None
    } for t in tasks])


@app.route('/api/v1/tasks/<int:id>')
@login_required
def api_task_detail(id):
    task = Task.query.get_or_404(id)
    return jsonify({
        'id': task.id,
        'name': task.name,
        'schedule_type': task.schedule_type,
        'schedule_config': task.get_schedule_config(),
        'enabled': task.enabled,
        'last_run_status': task.last_run_status,
        'last_run_time': task.last_run_time.isoformat() if task.last_run_time else None,
        'next_run_time': task.next_run_time.isoformat() if task.next_run_time else None
    })


@app.route('/api/v1/tasks/<int:id>/run', methods=['POST'])
@login_required
def api_task_run(id):
    task = Task.query.get_or_404(id)
    execute_task_script(task.id)
    log_audit('run', 'task', task.id)
    return jsonify({'success': True})


@app.route('/api/v1/flows')
@login_required
def api_flows():
    flows = Flow.query.all()
    return jsonify([{
        'id': f.id,
        'name': f.name,
        'schedule_type': f.schedule_type,
        'enabled': f.enabled,
        'last_run_status': f.last_run_status,
        'next_run_time': f.next_run_time.isoformat() if f.next_run_time else None
    } for f in flows])


@app.route('/api/v1/flows/<int:id>')
@login_required
def api_flow_detail(id):
    flow = Flow.query.get_or_404(id)
    return jsonify({
        'id': flow.id,
        'name': flow.name,
        'schedule_type': flow.schedule_type,
        'schedule_config': flow.get_schedule_config(),
        'enabled': flow.enabled,
        'nodes': flow.get_nodes(),
        'connections': flow.get_connections(),
        'last_run_status': f.last_run_status,
        'last_run_time': flow.last_run_time.isoformat() if flow.last_run_time else None,
        'next_run_time': flow.next_run_time.isoformat() if flow.next_run_time else None
    })


@app.route('/api/v1/health')
@login_required
def api_health():
    return jsonify({
        'status': 'running',
        'scheduler_running': scheduler.running,
        'uptime': 'active'
    })


def init_db():
    with app.app_context():
        db.create_all()
        if not User.query.first():
            admin = User(username='admin')
            admin.set_password('admin123')
            db.session.add(admin)
            db.session.commit()
            print("Created default user: admin / admin123")


def init_scheduler():
    scheduler.start()
    reload_schedules()
    print("Scheduler started")


if __name__ == '__main__':
    init_db()
    init_scheduler()
    app.run(host='0.0.0.0', port=5000, debug=True)

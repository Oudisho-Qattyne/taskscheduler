"""Database models for Task Scheduler."""
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime
import json

db = SQLAlchemy()


class User(db.Model):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password_hash = db.Column(db.String(256), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)


class Task(db.Model):
    __tablename__ = 'tasks'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(200), nullable=False)
    script_path = db.Column(db.String(500), nullable=True)
    schedule_type = db.Column(db.String(50), nullable=False)
    schedule_config = db.Column(db.Text, default='{}')
    enabled = db.Column(db.Boolean, default=True)
    timeout_seconds = db.Column(db.Integer, default=30)
    env_vars = db.Column(db.Text, default='{}')
    cli_args = db.Column(db.Text, default='')
    tags = db.Column(db.String(500), default='')
    max_retries = db.Column(db.Integer, default=0)
    max_instances = db.Column(db.Integer, default=1)
    last_run_time = db.Column(db.DateTime, nullable=True)
    last_run_status = db.Column(db.String(20), nullable=True)
    last_run_output = db.Column(db.Text, nullable=True)
    next_run_time = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def get_schedule_config(self):
        return json.loads(self.schedule_config) if self.schedule_config else {}

    def set_schedule_config(self, config):
        self.schedule_config = json.dumps(config)

    def get_env_vars(self):
        return json.loads(self.env_vars) if self.env_vars else {}

    def set_env_vars(self, env_vars):
        self.env_vars = json.dumps(env_vars)


class Flow(db.Model):
    __tablename__ = 'flows'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(200), nullable=False)
    schedule_type = db.Column(db.String(50), nullable=False)
    schedule_config = db.Column(db.Text, default='{}')
    enabled = db.Column(db.Boolean, default=True)
    nodes = db.Column(db.Text, default='[]')
    connections = db.Column(db.Text, default='[]')
    timeout_seconds = db.Column(db.Integer, default=30)
    max_retries = db.Column(db.Integer, default=0)
    last_run_time = db.Column(db.DateTime, nullable=True)
    last_run_status = db.Column(db.String(20), nullable=True)
    last_run_output = db.Column(db.Text, nullable=True)
    last_run_node_errors = db.Column(db.Text, default='{}')
    next_run_time = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def get_nodes(self):
        return json.loads(self.nodes) if self.nodes else []

    def set_nodes(self, nodes):
        self.nodes = json.dumps(nodes)

    def get_connections(self):
        return json.loads(self.connections) if self.connections else []

    def set_connections(self, connections):
        self.connections = json.dumps(connections)

    def get_schedule_config(self):
        return json.loads(self.schedule_config) if self.schedule_config else {}

    def set_schedule_config(self, config):
        self.schedule_config = json.dumps(config)

    def get_node_errors(self):
        return json.loads(self.last_run_node_errors) if self.last_run_node_errors else {}

    def set_node_errors(self, errors):
        self.last_run_node_errors = json.dumps(errors)


class Function(db.Model):
    __tablename__ = 'functions'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), unique=True, nullable=False)
    description = db.Column(db.Text, nullable=True)
    code = db.Column(db.Text, nullable=False)
    params = db.Column(db.Text, default='[]')
    return_type = db.Column(db.String(50), default='any')
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def get_params(self):
        return json.loads(self.params) if self.params else []

    def set_params(self, params):
        self.params = json.dumps(params)


class Constant(db.Model):
    __tablename__ = 'constants'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), unique=True, nullable=False)
    value = db.Column(db.Text, nullable=False)
    type = db.Column(db.String(20), default='str')
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ExecutionLog(db.Model):
    __tablename__ = 'execution_logs'
    id = db.Column(db.Integer, primary_key=True)
    task_id = db.Column(db.Integer, db.ForeignKey('tasks.id'), nullable=True)
    flow_id = db.Column(db.Integer, db.ForeignKey('flows.id'), nullable=True)
    run_time = db.Column(db.DateTime, default=datetime.utcnow)
    status = db.Column(db.String(20), nullable=False)
    output = db.Column(db.Text, nullable=True)
    duration_ms = db.Column(db.Integer, nullable=True)
    attempt_number = db.Column(db.Integer, default=1)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class AuditLog(db.Model):
    __tablename__ = 'audit_logs'
    id = db.Column(db.Integer, primary_key=True)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)
    username = db.Column(db.String(80), nullable=False)
    action = db.Column(db.String(50), nullable=False)
    entity_type = db.Column(db.String(50), nullable=False)
    entity_id = db.Column(db.Integer, nullable=True)
    details = db.Column(db.Text, nullable=True)

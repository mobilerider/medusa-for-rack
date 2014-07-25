import json
import getpass
from os import environ
from uuid import uuid4
from os.path import dirname, basename
from pprint import pprint
from StringIO import StringIO
from time import sleep
from threading import Thread
from urlparse import urlparse, urlunparse

from fabric.api import env, sudo, run, cd, put, execute, prompt
from fabric.decorators import task, parallel
from fabric.utils import _AttributeDict as AttributeDict

import pyrax
from rackspace_monitoring.providers import get_driver as get_monitoring_driver
from rackspace_monitoring.types import Provider as MonitoringProvider


env.disable_known_hosts = True
env.connection_attempts = 5


def add_hooks(method):
    '''
    Simple decorator to add a `before_<method name>` and a `after_<method name>`
    to a class. Every time the `method` is called on the class, if a method
    named `before_<method name>` exists, it is called with the original
    arguments, before calling the actual method. The same if a method named
    `after_<method name>` it is called after the actual method is executed.
    '''

    def with_hooks(self, *args, **kwargs):
        try:
            getattr(self, 'before_' + method.__name__)(*args, **kwargs)
        except (AttributeError, TypeError):
            pass
        method_result = method(self, *args, **kwargs)
        try:
            getattr(self, 'after_' + method.__name__)(*args, **kwargs)
        except (AttributeError, TypeError):
            pass
        return method_result

    with_hooks.__name__ = method.__name__ + '_with_hooks'
    return with_hooks


def ensure_settings(*settings):
    assert \
        [setting for setting in settings if isinstance(setting, basestring)], \
        '@ensure_settings: All arguments must be strings: ' + \
        'the setting keys that have to be set in settings'

    def actual_decorator(method):

        def method_with_settings(self, *args, **kwargs):
            for setting in settings:
                if not self.settings.get(setting, None):
                    raise ValueError('Setting `{setting}` must be set before calling `{method}`'.format(
                        setting=setting,
                        method=method.__name__,
                    ))
            return method(self, *args, **kwargs)

        method_with_settings.__name__ = method.__name__ + '_with_settings'
        return method_with_settings

    return actual_decorator


class ServerWatcherThread(Thread):
    expected_statuses = ['ACTIVE', 'ERROR', 'available', 'COMPLETED']

    def __init__(self, deployer, server, initial_ip=None, callback=None,
                 *callback_args, **callback_kwargs):
        super(ServerWatcherThread, self).__init__()
        self.server = server
        self.deployer = deployer
        self.initial_ip = initial_ip
        self.callback = callback
        self.callback_args = callback_args
        self.callback_kwargs = callback_kwargs

    @staticmethod
    def server_public_addr(server):
        try:
            return [addr for addr in server.addresses['public'] if addr['version'] == 4][0]['addr']
        except (KeyError, IndexError):
            return getattr(server, 'accessIPv4', None)

    def run(self):
        while True:
            sleep(2)
            self.server = self.deployer.cloudservers.servers.get(self.server.id)
            if not self.server.status in self.expected_statuses:
                print 'Waiting for server {name} ({id}) to be ready... (Status: {status})'.format(
                    name=self.server.name,
                    id=self.server.id,
                    status=self.server.status,
                    address=getattr(self.server, 'accessIPv4', self.initial_ip),
                )
                continue

            if getattr(self.server, 'accessIPv4', self.initial_ip) == self.server_public_addr(self.server):
                print 'Waiting for server {name} ({id}) to be accesible... (Current: {address})'.format(
                    name=self.server.name,
                    id=self.server.id,
                    status=self.server.status,
                    address=getattr(self.server, 'accessIPv4', self.initial_ip),
                )
                continue

            # All tests passed, break the pooling cycle
            break

        if self.callback:
            self.callback(self.server, *self.callback_args, **self.callback_kwargs)


class Deployer(object):

    SETTINGS_KEYS = (
        'name_prefix', 'server_count', 'flavor_id', 'distro_id', 'git_repo',
        'destination_dir', 'copy_files', 'apt_packages', 'post_install',
        'restart_services', 'rackspace_username', 'rackspace_apikey',
        'ssh_user', 'ssh_key_name', 'copy_ssh_public_key', 'file_permissions',
        'after_git_repo', 'git_repo_username', 'git_repo_password',
    )

    GIT_HOSTS = ('bitbucket.org', 'github.com', )

    def __init__(self, task_kwargs=None, json_config_file=None):
        json_config_file = json_config_file or './deploy_settings.json'
        self.cloudservers = None
        self.settings = AttributeDict({})
        self.fabric_env_servers = []
        self.created_servers = []

        task_kwargs = task_kwargs or {}

        settings = self.read_settings_file(json_config_file)

        for key in self.SETTINGS_KEYS:
            try:
                self.settings[key] = task_kwargs[key]
            except KeyError:
                try:
                    self.settings[key] = environ[key]
                except KeyError:
                    try:
                        self.settings[key] = settings[key]
                    except KeyError:
                        pass

        self.settings.server_count = int(self.settings.server_count)
        self.settings.setdefault('ssh_user', 'root')
        self.ensure_settings('rackspace_username', 'rackspace_apikey')

        pyrax.set_setting('identity_type', 'rackspace')
        pyrax.set_credentials(
            self.settings.rackspace_username,
            self.settings.rackspace_apikey)
        self.cloudservers = pyrax.connect_to_cloudservers()

    @property
    def is_root(self):
        return self.settings.get('ssh_user', None) == 'root'

    def command(self, *args, **kwargs):
        return (run if self.is_root else sudo)(*args, **kwargs)

    def read_settings_file(self, json_config_file):
        try:
            with open(json_config_file, 'r') as json_fp:
                data = json.load(json_fp)
                assert isinstance(data, dict), \
                    'Settings file must be a valid JSON ' \
                    'file with a single mapping object. ({filename})'.format(
                        filename=json_config_file,
                    )
                return data
        except IOError as io_error:
            # print repr(io_error)
            pass
        return {}

    def ensure_settings(self, *keys):
        for key in keys:
            if not self.settings.get(key, None):
                raise ValueError('Setting `{key}` is not set'.format(key=key))

    def get_flavors(self):
        return self.cloudservers.flavors.list()

    def get_distros(self, distro_id=None):
        distros = self.cloudservers.list_base_images()
        if distro_id:
            for distro in distros:
                if distro_id == distro.id:
                    return distro
            return None
        return distros

    def get_servers(self):
        return self.cloudservers.servers.list()

    def create_servers(self):
        self.ensure_settings(
            'name_prefix', 'server_count', 'flavor_id', 'distro_id', 'ssh_user')

        flavor_obj = self.cloudservers.flavors.get(self.settings.flavor_id)
        distro_obj = self.cloudservers.images.get(self.settings.distro_id)

        for counter in xrange(self.settings.server_count):
            self.created_servers.append(self.cloudservers.servers.create(
                name=self.settings.name_prefix + '_' + uuid4().hex,
                image=distro_obj,
                flavor=flavor_obj,
                availability_zone=self.settings.get('availability_zone', 'DFW'),
                key_name=self.settings.get('ssh_key_name', None),
                networks=[
                    {'uuid': '11111111-1111-1111-1111-111111111111', },
                    {'uuid': '00000000-0000-0000-0000-000000000000', },
                ]
            ))
        return self.created_servers

    def wait_for_active(self, servers=None,
                        callback=None, *callback_args, **callback_kwargs):
        threads = []
        for server in servers or self.created_servers:
            thread = ServerWatcherThread(
                self, server, initial_ip=ServerWatcherThread.server_public_addr(server),
                callback=callback, *callback_args, **callback_kwargs)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        return [thread.server for thread in threads]

    def install_rackspace_agent(self):
        self.command('aptitude install curl -y -q=2')
        self.command('echo "deb http://stable.packages.cloudmonitoring.rackspace.com/ubuntu-14.04-x86_64 cloudmonitoring main" > /etc/apt/sources.list.d/rackspace-monitoring-agent.list', shell=True)
        self.command('curl https://monitoring.api.rackspacecloud.com/pki/agent/linux.asc | sudo apt-key add -', shell=True)
        self.command('aptitude update -q=2')
        self.command('aptitude install rackspace-monitoring-agent -y -q=2')

        monitoring_driver = get_monitoring_driver(MonitoringProvider.RACKSPACE)
        monitoring_driver_instance = monitoring_driver(self.settings.rackspace_username, self.settings.rackspace_apikey)
        token = monitoring_driver_instance.create_agent_token(label='generated-{prefix}-{server}'.format(
            prefix=self.settings.get('name_prefix', uuid4()),
            server=env.host_string,
        ))

        put(local_path=StringIO('monitoring_token {token}'.format(token=token.token)),
            remote_path='/etc/rackspace-monitoring-agent.cfg', use_sudo=self.is_root)
        self.command('service rackspace-monitoring-agent restart')

    def install_apt_packages(self, *packages):
        packages = packages or self.settings.get('apt_packages', None)
        if packages:
            self.command('aptitude update -q=2')
            self.command('aptitude install {packages} -y -q=2'.format(
                packages=' '.join(packages)
            ))

    def format_repo_url(self, repo_url=None, save=True):
        repo_url_parsed = urlparse(repo_url or self.settings.repo_url, 'https')
        settings_git_repo_username = self.settings.get('git_repo_username', repo_url_parsed.username or '')
        settings_git_repo_password = self.settings.get('git_repo_password', repo_url_parsed.password or '')
        auth_string = ''
        if settings_git_repo_username:
            auth_string = settings_git_repo_username
            if settings_git_repo_password:
                auth_string += ':' + settings_git_repo_password
            auth_string +=  '@'
        new_url = list(repo_url_parsed)
        new_url[1] = auth_string + \
            repo_url_parsed.hostname + \
            ((':' + str(repo_url_parsed.port)) if repo_url_parsed.port else '')
        new_url = urlunparse(new_url)
        if save:
            self.settings['git_repo'] = new_url
        return new_url

    def clone_repo(self):
        self.ensure_settings('git_repo', 'destination_dir')
        self.format_repo_url(self.settings.git_repo, save=True)

        destination_dir = self.settings.destination_dir
        self.install_apt_packages('git')
        parent_dir = dirname(destination_dir)
        clone_dir = basename(destination_dir)
        self.command('mkdir -p ' + destination_dir)
        with cd(parent_dir):
            result = self.command(
                'git clone -q {repo_url} {directory}'.format(
                    repo_url=self.settings.git_repo,
                    directory=clone_dir,
                ),
                warn_only=True
            )
        if not result.succeeded:
            with cd(destination_dir):
                self.command('git reset --hard')
                self.command('git pull')

        commands_after_git_repo = self.settings.get('after_git_repo', None)
        if isinstance(commands_after_git_repo, list):
            with cd(destination_dir):
                for cmd in commands_after_git_repo:
                    self.command(cmd, shell=True, warn_only=True)

    def copy_files(self):
        self.ensure_settings('destination_dir')
        copy_files_dict = self.settings.get('copy_files', {})
        assert isinstance(copy_files_dict, dict), 'The `copy_files` setting must be a mapping object'
        assert len([src_path for src_path in copy_files_dict.keys() if not src_path.startswith('/')]) == len(copy_files_dict.keys()), 'Some keys of the `copy_files` settings begin with a `/`. All source paths must be paths relative to the cloned repo.'
        assert len([dst_path for dst_path in copy_files_dict.values() if dst_path.startswith('/')]) == len(copy_files_dict.values()), 'Some keys of the `copy_files` settings do not begin with a `/` All destination paths must be absolute'

        with cd(self.settings.destination_dir):
            for src, dst in copy_files_dict.items():
                self.command('cp -Rv {src} {dst}'.format(src=src, dst=dst))

    def set_file_permissions(self):
        destination_dir = self.settings.get('destination_dir', '')
        permissions = self.settings.get('file_permissions', {})
        if destination_dir and permissions:
            with cd(destination_dir):
                for filename, attributes in permissions.items():
                    self.command('mkdir -p {dir}'.format(dir=dirname(filename)))
                    self.command('touch {file}'.format(file=filename))

                    if attributes.get('user', None) and attributes.get('group', None):
                        owner = attributes['user'] + ':' + attributes['group']
                    elif attributes.get('user', None):
                        owner = attributes['user']
                    elif attributes.get('group', None):
                        owner = attributes['group']
                    else:
                        owner = ''

                    if owner:
                        self.command('chown -R {owner} {file}'.format(
                            owner=owner,
                            file=filename,
                        ))

                    if attributes.get('mode', None):
                        self.command('chmod -R {mode} {file}'.format(
                            mode=attributes['mode'],
                            file=filename,
                        ))


    def restart_services(self):
        self.ensure_settings('restart_services')
        for service_name in self.settings.restart_services:
            self.command('service {service_name} restart'.format(service_name=service_name))


@task
def list_distros(**task_kwargs):
    deployer = Deployer(task_kwargs=task_kwargs)
    for distro in deployer.get_distros(distro_id=None):
        print '{name} ({id})'.format(id=distro.id, name=distro.name)


@task
def list_flavors(**task_kwargs):
    deployer = Deployer(task_kwargs=task_kwargs)
    for flavor in deployer.get_flavors():
        print '{name} ({id})'.format(id=flavor.id, name=flavor.name)


@task
def list_servers(**task_kwargs):
    deployer = Deployer(task_kwargs=task_kwargs)
    server_id = task_kwargs.get('server_id', None)
    for server in deployer.get_servers():
        if not server_id:
            print '{name} ({id}) Status: {status} [{address}]'.format(
                id=server.id,
                name=server.name,
                status=server.status,
                address=server.accessIPv4,
            )
        elif server.id == server_id:
            pprint(server.__dict__)


@task
def deploy(**task_kwargs):
    settings_file = task_kwargs.pop('settings_file', None)
    server_list = task_kwargs.pop('server_list', None)

    ask_for_repo_auth = task_kwargs.pop('ask_for_git_auth', False)
    if ask_for_repo_auth:
        print 'Enter the username/password for when cloning the Git repository ' + \
              ' (these will override any username/password already on the Git HTTPS URL)'
        git_repo_username = prompt('Repository username:', default=getpass.getuser())
        git_repo_password = getpass.getpass('Repository password: ')
        if git_repo_username:
            task_kwargs['git_repo_username'] = git_repo_username
            task_kwargs['git_repo_password'] = git_repo_password

    deployer = Deployer(task_kwargs=task_kwargs, json_config_file=settings_file)

    if isinstance(server_list, basestring):
        server_list = [server_list]

    def server_from_id(_id):
        for server in deployer.cloudservers.servers.list():
            for attr in ('name', 'id', 'accessIPv4', ):
                if getattr(server, attr, None) == _id:
                    return server

    if server_list:
        server_list = [srv for srv in [server_from_id(_id) for _id in server_list] if srv]
        assert server_list, 'Invalid server list'
    else:
        server_list = deployer.create_servers()

    env.user = deployer.settings.ssh_user

    server_list = deployer.wait_for_active(server_list)

    def start_deploy():
        # deployer.install_rackspace_agent()
        deployer.install_apt_packages()
        deployer.clone_repo()
        deployer.copy_files()
        deployer.set_file_permissions()
        deployer.restart_services()

    execute(parallel(start_deploy), hosts=[srv.accessIPv4 for srv in server_list])


@task
def delete_servers(*ids, **task_kwargs):
    deployer = Deployer(task_kwargs=task_kwargs)
    for server in ids:
        try:
            server = deployer.cloudservers.servers.get(server)
            print server.delete()
            # print server.name
        except Exception, e:
            print repr(e)

import json
from os import environ
from uuid import uuid4
from os.path import dirname, basename, join as path_join
from pprint import pprint
from StringIO import StringIO
from time import sleep

from fabric.api import env, sudo, run, cd, put, settings as fabric_settings
from fabric.decorators import task, hosts
import pyrax
from pyrax import connect_to_cloudservers
from pyrax.utils import wait_until as pyrax_wait_until


env.disable_known_hosts = True
env.connection_attempts = 5


def argparser_type(value):
    try:
        return json.loads(value)
    except ValueError, original_error:
        try:
            return json.loads('"' + value + '"')
        except ValueError:
            raise original_error


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


class Deployer(object):

    SETTINGS_KEYS = (
        'name_prefix', 'server_count', 'flavor_id', 'distro_id', 'git_repo',
        'destination_dir', 'copy_files', 'apt_packages', 'post_install',
        'restart_services', 'rackspace_username', 'rackspace_apikey',
        'ssh_user', 'ssh_key_name', 'copy_ssh_public_key', 'file_permissions',
        'after_git_repo',
    )

    GIT_HOSTS = ('bitbucket.org', 'github.com', )

    def __init__(self, task_kwargs=None, json_config_file='./deploy_settings.json'):
        self.cloudservers = None
        self.settings = {}
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

        self.settings['server_count'] = int(self.settings['server_count'])
        self.settings.setdefault('ssh_user', 'root')
        self.ensure_settings('rackspace_username', 'rackspace_apikey')

        pyrax.set_setting('identity_type', 'rackspace')
        pyrax.set_credentials(self.settings['rackspace_username'], self.settings['rackspace_apikey'])
        self.cloudservers = connect_to_cloudservers()

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

    def hosts_list(self, servers):
        return [server.accessIPv4 for server in servers]

    def create_servers(self):
        self.ensure_settings('name_prefix', 'server_count', 'flavor_id', 'distro_id', 'ssh_user')

        flavor_obj = self.cloudservers.flavors.get(self.settings['flavor_id'])
        distro_obj = self.cloudservers.images.get(self.settings['distro_id'])

        for counter in xrange(self.settings['server_count']):
            self.created_servers.append(self.cloudservers.servers.create(
                name=self.settings['name_prefix'] + '_' + uuid4().hex,
                image=distro_obj,
                flavor=flavor_obj,
                availability_zone=self.settings.get('availability_zone', 'DFW'),
                key_name=self.settings.get('ssh_key_name', None),
                networks=[
                    {'uuid': '11111111-1111-1111-1111-111111111111', },
                    {'uuid': '00000000-0000-0000-0000-000000000000', },
                ]
            ))

        for server in self.created_servers:
            pyrax_wait_until(
                server,
                'status',
                ['active', 'ACTIVE', ],
                interval=3,
                attempts=0,
                verbose=True
            )

        refreshed_servers = []
        for server_id in [srv.id for srv in self.created_servers]:
            server = self.cloudservers.servers.get(server_id)
            while getattr(server, 'accessIPv4', None) == [addr for addr in server.addresses['public'] if addr['version'] == 4][0]['addr']:
                print 'Waiting for server {name} ({id}) to update it\'s public address...'.format(
                    name=server.name,
                    id=server.id,
                )
                sleep(2)
                server = self.cloudservers.servers.get(server_id)
            refreshed_servers.append(server)

        self.created_servers = refreshed_servers
        self.fabric_env_servers = self.hosts_list(self.created_servers)
        return self.created_servers

    def install_rackspace_agent(self):
        self.command('aptitude install curl -y -q=2')
        self.command('echo "deb http://stable.packages.cloudmonitoring.rackspace.com/ubuntu-14.04-x86_64 cloudmonitoring main" > /etc/apt/sources.list.d/rackspace-monitoring-agent.list', shell=True)
        self.command('curl https://monitoring.api.rackspacecloud.com/pki/agent/linux.asc | sudo apt-key add -', shell=True)
        self.command('aptitude update -q=2')
        self.command('aptitude install rackspace-monitoring-agent -y -q=2')

        put(local_path=StringIO('monitoring_token {token}'.format(token=pyrax.identity.get_token())),
            remote_path='/etc/rackspace-monitoring-agent.cfg', use_sudo=self.is_root)
        self.command('service rackspace-monitoring-agent restart')

    def install_apt_packages(self, *packages):
        packages = packages or self.settings.get('apt_packages', None)
        if packages:
            self.command('aptitude update -q=2')
            self.command('aptitude install {packages} -y -q=2'.format(packages=' '.join(packages)))

    def copy_ssh_public_key(self):
        try:
            copy_ssh_public_key = self.settings['copy_ssh_public_key']
            if copy_ssh_public_key:
                put(local_path='~/.ssh/id_rsa', remote_path='~/.ssh/id_rsa', mode=0600, use_sudo=self.is_root)
                put(local_path='~/.ssh/id_rsa.pub', remote_path='~/.ssh/id_rsa.pub', mode=0600, use_sudo=self.is_root)
        except KeyError:
            pass

    def clone_repo(self):
        self.ensure_settings('git_repo', 'destination_dir', 'rackspace_username', 'rackspace_apikey')

        self.command('touch ~/.ssh/known_hosts')
        for ssh_host in self.GIT_HOSTS:
            self.command('ssh-keyscan -t rsa,dsa {ssh_host} | sort -u - ~/.ssh/known_hosts > ~/.ssh/tmp_hosts'.format(ssh_host=ssh_host), shell=True)
            self.command('cat ~/.ssh/tmp_hosts >> ~/.ssh/known_hosts', shell=True)

        destination_dir = self.settings['destination_dir']
        self.install_apt_packages('git')
        parent_dir = dirname(destination_dir)
        clone_dir = basename(destination_dir)
        self.command('mkdir -p ' + destination_dir)
        with cd(parent_dir):
            result = self.command(
                'git clone -q {repo_url} {directory}'.format(
                    repo_url=self.settings['git_repo'],
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
                    self.command(cmd, shell=True)

    def copy_files(self):
        self.ensure_settings('destination_dir')
        copy_files_dict = self.settings.get('copy_files', {})
        assert isinstance(copy_files_dict, dict), 'The `copy_files` setting must be a mapping object'
        assert len([src_path for src_path in copy_files_dict.keys() if not src_path.startswith('/')]) == len(copy_files_dict.keys()), 'Some keys of the `copy_files` settings begin with a `/`. All source paths must be paths relative to the cloned repo.'
        assert len([dst_path for dst_path in copy_files_dict.values() if dst_path.startswith('/')]) == len(copy_files_dict.values()), 'Some keys of the `copy_files` settings do not begin with a `/` All destination paths must be absolute'

        with cd(self.settings['destination_dir']):
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
        for service_name in self.settings['restart_services']:
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
    deployer = Deployer(task_kwargs=task_kwargs)
    created_servers = deployer.create_servers()

    for server in created_servers:
        with fabric_settings(host_string=server.accessIPv4, user=deployer.settings['ssh_user']):
            # deployer.install_rackspace_agent()
            deployer.copy_ssh_public_key()
            deployer.install_apt_packages()
            deployer.clone_repo()
            deployer.copy_files()
            deployer.set_file_permissions()
            deployer.restart_services()


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

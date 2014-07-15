import json
from os import environ
from uuid import uuid4
from os.path import dirname, basename, join as path_join
from pprint import pprint
from StringIO import StringIO

from fabric.api import env, sudo, run, cd, put
from fabric.decorators import task
from fab_utils.fabfile.apt import install as apt_install
import pyrax
from pyrax import connect_to_cloudservers
from pyrax.utils import wait_until as pyrax_wait_until


NOT_SPECIFIED = object()

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
        'ssh_user', 'ssh_key_name'
    )

    def __init__(self, task_kwargs=None, json_config_file='./deploy_settings.json'):
        self.cloudservers = None
        self.settings = {}
        self.fabric_env_servers = []

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

    # @add_hooks
    def get_servers(self):
        return self.cloudservers.servers.list()

    def hosts_list(self, servers):
        return [sorted(server.networks['public'], key=len)[0] for server in servers]

    def create_servers(self):
        self.ensure_settings('name_prefix', 'server_count', 'flavor_id', 'distro_id', 'ssh_user')

        flavor_obj = self.cloudservers.flavors.get(self.settings['flavor_id'])
        distro_obj = self.cloudservers.images.get(self.settings['distro_id'])

        self.created_servers = []
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

        self.fabric_env_servers = self.hosts_list(self.created_servers)

        env.hosts = self.fabric_env_servers
        env.user = self.settings['ssh_user']

        return self.created_servers

    def install_rackspace_agent(self):
        if hasattr(self, 'fabric_env_servers'):
            sudo('echo "deb http://stable.packages.cloudmonitoring.rackspace.com/ubuntu-14.04-x86_64 cloudmonitoring main" > /etc/apt/sources.list.d/rackspace-monitoring-agent.list', shell=True)
            sudo('curl https://monitoring.api.rackspacecloud.com/pki/agent/linux.asc | sudo apt-key add -', shell=True)
            sudo('aptitude update')
            apt_install('rackspace-monitoring-agent')
            put(local_path=StringIO('monitoring_token {token}'.format(token=pyrax.identity.identity.get_token())),
                remote_path='/etc/rackspace-monitoring-agent.cfg', use_sudo=True)
            sudo('service rackspace-monitoring-agent restart')

    def install_apt_packages(self):
        packages = self.settings.get('apt_packages', None)
        if packages and hasattr(self, 'fabric_env_servers'):
            env.hosts = self.fabric_env_servers
            sudo('aptitude update')
            sudo('aptitude safe-upgrade')
            apt_install(packages)

    def clone_repo(self):
        self.ensure_settings('git_repo', 'destination_dir', 'rackspace_username', 'rackspace_apikey')
        destination_dir = self.settings['destination_dir']
        if hasattr(self, 'fabric_env_servers'):
            self.install_apt_packages('git')
            parent_dir = dirname(destination_dir)
            clone_dir = basename(destination_dir)
            sudo('mkdir -p ' + destination_dir)
            with cd(parent_dir):
                sudo('git clone {repo_url} {directory}'.format(
                    repo_url=self.settings['git_repo'],
                    directory=clone_dir,
                ))

    def copy_files(self):
        self.ensure_settings('destination_dir')
        env.hosts = self.fabric_env_servers
        copy_files_dict = self.settings.get('copy_files', {})
        assert isinstance(copy_files_dict, dict), 'The `copy_files` setting must be a mapping object'
        assert len([src_path for src_path in copy_files_dict.keys() if not src_path.startswith('/')]) == len(copy_files_dict.keys()), 'Some keys of the `copy_files` settings begin with a `/`. All source paths must be paths relative to the cloned repo.'
        assert len([dst_path for dst_path in copy_files_dict.values() if dst_path.startswith('/')]) == len(copy_files_dict.values()), 'Some keys of the `copy_files` settings do not begin with a `/` All destination paths must be absolute'

        for src, dst in copy_files_dict.items():
            sudo('cp -Rv {src} {dst}'.format(src=src, dst=dst))

    def restart_services(self, servers):
        self.ensure_settings('restart_services')
        env.hosts = self.hosts_list(servers)
        for service_name in self.settings['restart_services']:
            sudo('service {service_name} restart'.format(service_name=service_name))


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
            print '{name} ({id}) Status: {status}'.format(
                id=server.id,
                name=server.name,
                status=server.status,
                # networks=json.dumps(server.networks),  #  Networks: {networks}
            )
        elif server.id == server_id:
            pprint(server.__dict__)


@task
def deploy(**task_kwargs):
    deployer = Deployer(task_kwargs=task_kwargs)
    pprint(deployer.settings)
    created_servers = deployer.create_servers()
    print deployer.fabric_env_servers

    print json.dumps([
        {
            # 'address': sorted(server.networks['public'], key=len)[0],
            'networks': server.networks,
            'name': server.name,
            'id': server.id,
        }
        for server in created_servers])

    deployer.install_rackspace_agent()
    deployer.install_apt_packages()
    deployer.clone_repo()
    deployer.copy_files()

    # for server in created_servers:
    #     server.delete()

    # for server in created_servers:
    #     pyrax_wait_until(
    #         server,
    #         'status',
    #         ['deleted', 'DELETED', ],
    #         interval=3,
    #         attempts=0,
    #         verbose=True
    #     )


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
#!/bin/env python
'''
ceph admin interface
'''

import sys
import os
import json
import logging
import logging.config
from errno import EINVAL, EPERM, ENOENT

import rados
import cephfs

__all__ = ['set_log_conf_file','version','connect','lsuser','getuser',
    'adduser','updateuser','deluser','getuser_usage','get_cluster_usage',
    'set_root_prefix']

home_dir = os.getenv('CEPH_ADMIN_HOME', '.')
version_str = os.getenv('CEPH_ADMIN_VERSION', '0.0.1')
default_log_conf = os.path.join(home_dir, 'conf', 'logging.conf')
default_log_file = os.path.join(home_dir, 'logs', 'admin.log')
default_admin_conf = os.path.join(home_dir, 'conf', 'admin.info')

units = {'b':1,'k':1024,'m':1024*1024,'g':1024*1024*1024,'t':1024*1024*1024*1024}
default_unit = 'g'
root_prefix = '/'

if sys.version_info[0] == 2:
    import codecs
    open = codecs.open

def set_root_prefix(prefix):
    if not prefix:
        prefix = '/'
    if prefix[-1] != '/':
        prefix += '/'
    global root_prefix
    root_prefix = prefix

def set_log_conf_file(log_conf):
    if os.path.exists(log_conf):
        logging.config.fileConfig(log_conf,
                defaults={'logfilename':default_log_file})
    else:
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console.setFormatter(formatter)
        log.addHandler(console)

set_log_conf_file(default_log_conf)
log = logging.getLogger('cephadmin')

def version():
    return version_str

def _get_default_unit(unit):
    if unit is None:
        return default_unit
    unit = unit[0].lower()
    unit = unit if unit in units else default_unit
    return unit

def format_bytes(b):
    b = float(b)
    if b < 0:
        return b
    for unit in ['','Ki','Mi','Gi','Ti']:
        if b < 1024.0:
            return '%.1f%sB' % (b, unit)
        b /= 1024.0
    return '%.1fPiB' % b

def _uniq(seq):
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]

class Config(object):
    def __init__(self, cephaddr=None, admin_user=None, key=None):
        self.cephaddr = cephaddr
        self.admin_user = admin_user
        self.key = key

    def __repr__(self):
        return 'Config("cephaddr":"{0}","user":"{1}","key":"{2}")'\
            .format(self.cephaddr, self.admin_user, self.key)

    def to_json(self):
        return json.dumps(self.__dict__)

    def to_file(self, file_name):
        try:
            with open(file_name, 'w') as fp:
                json.dump(self.__dict__, fp)
        except Exception as e:
            log.error('Unable write conf file: %s', e)

    def to_dict(self):
        return {'mon host':self.cephaddr,'key':self.key}

    @classmethod
    def from_file(cls, file_name):
        try:
            with open(file_name, 'r') as fp:
                d = json.load(fp)
                return cls(**d)
        except Exception as e:
            log.error('Unable read conf file: %s', e)
        return cls()

class InvalidArgumentError(Exception):
    pass

class ConnectError(IOError):
    pass

class ListUserError(IOError):
    pass

class GetUserError(IOError):
    pass

class AddUserError(IOError):
    pass

class UpdateUserError(IOError):
    pass

class DelUserError(IOError):
    pass

class AttrError(IOError):
    pass

'''
param configfile: str admin user config file
param cephaddr: str ceph mon addr
param admin_user: str admin user
param key: str key for admin user
param verbose: verbose info

raises: InvalidArgumentError
return: rados instanse
'''
def connect(**kwargs):
    verbose = kwargs.pop('verbose', False)
    configfile = kwargs.pop('configfile', default_admin_conf)
    #load admin config from file
    config = Config.from_file(configfile)
    if verbose:
        log.info('load config file: %s', config)
    #read admin config from kwargs
    cephaddr = kwargs.get('cephaddr')
    if cephaddr:
        config.cephaddr = cephaddr
    admin_user = kwargs.get('admin_user')
    if admin_user:
        config.admin_user = admin_user
    key = kwargs.get('key')
    if key:
        config.key = key
    if config.cephaddr is None:
        raise InvalidArgumentError('require ceph mon host')
    if config.key is None:
        raise InvalidArgumentError('require admin user key')
    if config.admin_user is None:
        config.admin_user = 'admin'
    if verbose:
        log.info("admin config: %s", config.to_json())
    prefix = kwargs.get('prefix')
    if prefix:
        set_root_prefix(prefix)
    try:
        rd = rados.Rados(rados_id=config.admin_user, conf=config.to_dict())
        rd.connect()
    except Exception as e:
        log.error('connect ceph error: %s', e)
        raise ConnectError(e)
    if rd.state != 'connected':
        raise ConnectError('ceph not connected')
    #write the conf to file
    config.to_file(configfile)
    return rd, config

def login(func):
    def wrapper(**kwargs):
        rd = kwargs.get('rados')
        if rd is None or rd.state != 'connected':
            #not connected to rados
            kwargs['rados'], kwargs['config'] = connect(**kwargs)
        return func(**kwargs)
    return wrapper

'''
param: see connect function
'''
@login
def lsuser(**kwargs):
    rd = kwargs.pop('rados')
    reuse = kwargs.get('reuse', False)
    try:
        cmd = {'prefix':'auth ls',
               'format':'json'}
        ret, buf, out = rd.mon_command(json.dumps(cmd), '')
        if ret != 0:
            log.error('ls user error: %s', out)
            raise ListUserError(out)
        users = json.loads(buf.decode('utf8'))['auth_dump']
        ret = []
        for u in users:
            name = u['entity']
            if (name.startswith('client') and 
                not name.startswith('client.bootstrap') and
                name != 'client.admin'):
                ret.append(name[7:])
        return ret
    finally:
        if not reuse:
            rd.shutdown()

def __get_user_info(rd, user, verbose):
    cmd = {'prefix':'auth get',
           'entity':'client.'+user,
           'format':'json'}
    ret, buf, out = rd.mon_command(json.dumps(cmd), '')
    if ret != 0 or len(buf) == 0:
        log.error('get user error: %s', out)
        raise GetUserError(out)
    info = json.loads(buf.decode('utf8'))[0]
    if verbose:
        log.info('get user return info: %s', info)
    mds = info['caps']['mds'] if info.get('caps') and info['caps'].get('mds') else None
    return mds, info['key']

#login cephfs to set quota
def _set_quota_path(rados_instanse, path, quota, unit, verbose):
    try:
        fs = cephfs.LibCephFS(rados_inst=rados_instanse)
        fs.mount()
        fs.conf_set('client_permissions', '0')
        try:
            fs.mkdirs(path, 0o777)
        except cephfs.ObjectExists:
            pass
        quota = quota * units.get(unit)
        fs.setxattr(path, "ceph.quota.max_bytes", str(quota).encode('ascii'), 0)
        if verbose:
            log.info('set path {0} quota max bytes {1}'.format(path, quota))
    except Exception as e:
        log.error('set path {0} quota error: {1}'.format(path, e))
        raise AttrError(e)

def _set_quota(rados_instanse, user, quota, unit, verbose):
    path = os.path.join(root_prefix, user)
    _set_quota_path(rados_instanse, path, quota, unit, verbose)

def _get_path_used(fs, path):
    '''
    get path used
    required cephfs
    '''
    try:
        used = fs.getxattr(path, "ceph.dir.rbytes")
    except Exception as e:
        log.warning('get path {0} rbytes: {1}'.format(path, e))
        used = '0'
    try:
        quota = fs.getxattr(path, "ceph.quota.max_bytes")
    except Exception as e:
        log.warning('get path {0} quota error: {1}'.format(path, e))
        quota = '0'
    return used, quota, path

def _get_used_one_user(fs, user):
    path = os.path.join(root_prefix, user)
    return _get_path_used(fs, path)

def _get_users_used(rados_instanse, mds):
    '''
    get all users used from mds
    login cephfs to get used
    required rados
    '''
    try:
        fs = cephfs.LibCephFS(rados_inst=rados_instanse)
        fs.mount()
        paths = _get_paths_from_mds(mds)
        if paths:
            set_root_prefix(os.path.dirname(paths[-1]))
        return [_get_path_used(fs, p) for p in paths]
    except Exception as e:
        log.error('connect cephfs error: {0}'.format(e))
        raise AttrError(e)

def _get_user_used(rados_instanse, user):
    '''
    get one user used
    login cephfs to get used
    required rados
    '''
    try:
        fs = cephfs.LibCephFS(rados_inst=rados_instanse)
        fs.mount()
        path = os.path.join(root_prefix, user)
        return _get_path_used(fs, path)
    except Exception as e:
        log.error('connect cephfs error: {0}'.format(e))
        raise AttrError(e)

def _get_paths_from_mds(mds):
    groups = mds.split(', ')
    pre = 'allow rw path='
    return [g[len(pre):] for g in groups if g.startswith(pre)]

def _get_groups_from_mds(mds):
    groups = mds.split(', ')
    pre_path = 'allow rw path=' + root_prefix
    return [os.path.basename(g) for g in groups if g.startswith(pre_path)]

def _get_mds_from_groups(groups):
    def __get_path(g):
        return 'allow rw path=' + os.path.join(root_prefix, g)
    return ', '.join([__get_path(g) for g in groups if len(g) > 0])

def _get_mds_from_paths(paths):
    return ', '.join(['allow rw path=' + p for p in paths if len(p) > 0])

'''
param: see connect function
return dict of list [{'user':,'used':,'quota':}]
'''
@login
def get_all_users(**kwargs):
    rd = kwargs.pop('rados')
    reuse = kwargs.get('reuse', False)
    try:
        cmd = {'prefix':'auth ls',
               'format':'json'}
        ret, buf, out = rd.mon_command(json.dumps(cmd), '')
        if ret != 0:
            log.error('ls user error: %s', out)
            raise ListUserError(out)
        users = json.loads(buf.decode('utf8'))['auth_dump']
        fs = cephfs.LibCephFS(rados_inst=rd)
        fs.mount()
        names = []
        for u in users:
            name = u['entity']
            if (name.startswith('client') and 
                not name.startswith('client.bootstrap') and
                name != 'client.admin'):
                names.append(name[7:])
        def to_dict(name, used):
            return {'user':name, 'used':used[0], 'quota':used[1]}
        return [to_dict(n, _get_used_one_user(fs, n)) for n in names]
    finally:
        if not reuse:
            rd.shutdown()

'''
param user: str required user in cephfs
showpath: show path not groups
other params see connect function

return key,groups,used  (str,list,list) if showpath is false
return key,used  (str,list) if showpath is true
used is list of {used, quota, path}
'''
@login
def getuser(**kwargs):
    rd = kwargs.pop('rados')
    verbose = kwargs.pop('verbose', False)
    reuse = kwargs.get('reuse', False)
    showpath = kwargs.get('showpath', False)
    try:
        user = kwargs.pop('user')
        if user == 'admin':
            return '', [], []
        mds, key = __get_user_info(rd, user, verbose)
        if showpath:
            if mds:
                used = _get_users_used(rd, mds)
                return key, used
            else:
                return key, []
        else:
            groups, used = [], []
            if mds:
                groups = _get_groups_from_mds(mds)
                used = _get_users_used(rd, mds)
                try:
                    groups.remove(user)
                except ValueError:
                    pass
            return key, groups, used
    finally:
        if not reuse:
            rd.shutdown()

@login
def get_cluster_usage(**kwargs):
    rd = kwargs.pop('rados')
    verbose = kwargs.pop('verbose', False)
    reuse = kwargs.get('reuse', False)
    try:
        usage = rd.get_cluster_stats()
        return usage.get('kb_used'),usage.get('kb')
    finally:
        if not reuse:
            rd.shutdown()

@login
def getuser_usage(**kwargs):
    rd = kwargs.pop('rados')
    reuse = kwargs.get('reuse', False)
    try:
        user = kwargs.pop('user')
        return _get_user_used(rd, user)
    finally:
        if not reuse:
            rd.shutdown()

'''
param user: str required user in cephfs
other params see connect function

return str, user info config for cephcli
'''
@login
def exportuser(**kwargs):
    rd = kwargs.pop('rados')
    verbose = kwargs.pop('verbose', False)
    reuse = kwargs.get('reuse', False)
    root = kwargs.get('root')
    rootindex = kwargs.get('rootindex')
    rootindex = rootindex if rootindex else -1
    try:
        user = kwargs.pop('user')
        if user == 'admin':
            return '', [], []
        mds, key = __get_user_info(rd, user, verbose)
        paths = _get_paths_from_mds(mds)
        if paths and root not in paths:
            try:
                root = paths[rootindex]
            except IndexError:
                root = os.path.join(root_prefix, user)
                pass
        userinfo = {'cephconf': None,
                'root': root, 
                'name': user,
                'key': key,
                'cephaddr': kwargs.get('config').cephaddr 
                }
        return json.dumps(userinfo)
    finally:
        if not reuse:
            rd.shutdown()
    
'''
param user: str required user in cephfs
param groups: list groups 
param quota: quota
param unit: quota unit
param outkey: output file of key
other params see connect function

return key
'''
@login
def adduser(**kwargs):
    rd = kwargs.pop('rados')
    verbose = kwargs.pop('verbose', False)
    reuse = kwargs.get('reuse', False)
    try:
        user = kwargs.pop('user')
        cmd = {'prefix':'auth get-or-create',
               'entity':'client.'+user,
               'caps':['mon','allow r','mgr','allow r','osd',
                    'allow rw pool=cephfs_data','mds'],
               'format':'json'}
        groups = kwargs.get('groups')
        paths = kwargs.get('paths')
        if not paths and root_prefix == '/':
            log.warning('use default root prefix /')
        mds = None
        if paths:
            paths = _uniq(paths)
            mds_tmp = _get_mds_from_paths(paths)
            mds = mds + ', ' + mds_tmp if mds else mds_tmp
        if groups:
            groups.append(user)
            groups = _uniq(groups)
            mds_tmp = _get_mds_from_groups(groups)
            mds = mds + ', ' + mds_tmp if mds else mds_tmp
        if not mds:
            mds = _get_mds_from_groups([user])
        cmd['caps'].append(mds)
        ret, buf, out = rd.mon_command(json.dumps(cmd), '')
        if ret != 0 or len(buf) == 0:
            log.error('add user error: %s', out)
            raise AddUserError(out)
        info = json.loads(buf.decode('utf8'))[0]
        if verbose:
            log.info('add user return info: %s', info)
        outkey = kwargs.get('outkey')
        if outkey is not None:
            with open(outkey) as f:
                f.write(info['key'])
        log.info('add user {0} successfully\n\tkey: {1}'.format(user, info['key']))
        quota = kwargs.get('quota')
        if not quota:
            quota = 0
        unit = _get_default_unit(kwargs.get('unit'))
        if verbose:
            log.info('start to set user quota')
        _set_quota(rd, user, quota, unit, verbose)
        pu = unit.upper()
        log.info('set user {2} quota {0}{1}B successfully'
            .format(quota, pu+'i' if pu != 'B' else '', user))
        if paths:
            for p in paths:
                _set_quota_path(rd, p, quota, unit, verbose)
        return info['key']
    finally:
        if not reuse:
            rd.shutdown()

'''
param user: str required user in cephfs
param groups: list groups 
param quota: quota
param unit: quota unit
other params see connect function

return 0
'''
@login
def updateuser(**kwargs):
    rd = kwargs.pop('rados')
    verbose = kwargs.pop('verbose', False)
    reuse = kwargs.get('reuse', False)
    try:
        user = kwargs.pop('user')
        if user == 'admin':
            msg = 'Unable to update admin user'
            log.error(msg)
            raise UpdateUserError(msg)
        groups = kwargs.get('groups')   # share paths, dont set quota
        paths = kwargs.get('paths')     # my path
        pathrm = kwargs.get('pathrm')
        pathadd = kwargs.get('pathadd')
        all_paths = []
        new_paths = [] # for set quota
        def proc_paths(paths):
            if paths:
                paths = _uniq(paths)
                for i, p in enumerate(paths):
                    if p and p[0] != '/':
                        paths[i] = '/' + p;
            return paths
        if paths:
            paths = proc_paths(paths)
            all_paths.extend(paths)
            new_paths.extend(paths)
        elif pathrm or pathadd:
            mds, _ = __get_user_info(rd, user, verbose)
            if mds:
                old_paths = _get_paths_from_mds(mds)
                if pathrm:
                    pathrm = proc_paths(pathrm)
                    for p in pathrm:
                        if p in old_paths:
                            old_paths.remove(p)
                if pathadd:
                    pathadd = proc_paths(pathadd)
                    for p in pathadd:
                        if p not in old_paths:
                            old_paths.append(p)
                            new_paths.append(p)
                all_paths.extend(old_paths)
            else:
                if pathadd:
                    pathadd = proc_paths(pathadd)
                    all_paths.extend(pathadd)
                    new_paths.extend(pathadd)
        if groups:
            groups.append(user)
            groups = _uniq(groups)
            groups_paths = [os.path.join(root_prefix, g) for g in groups]
            all_paths.extend(groups_paths)
        if all_paths:
            cmd = {'prefix':'auth caps',
                   'entity':'client.'+user,
                   'caps':['mon','allow r','mgr','allow r','osd',
                        'allow rw pool=cephfs_data','mds'],
                   'format':'json'}
            all_paths = _uniq(all_paths)
            mds = _get_mds_from_paths(all_paths)
            cmd['caps'].append(mds)
            ret, buf, out = rd.mon_command(json.dumps(cmd), '')
            if ret != 0 or 'updated caps' not in out:
                log.error('update user error: %s', out)
                raise UpdateUserError(out)
        quota = kwargs.get('quota')
        if not quota:
            quota = 0
        unit = _get_default_unit(kwargs.get('unit'))
        _set_quota(rd, user, quota, unit, verbose)
        if new_paths:
            new_paths = _uniq(new_paths)
            for p in new_paths:
                _set_quota_path(rd, p, quota, unit, verbose)
        log.info('update user {0} successfully'.format(user))
        return 0
    finally:
        if not reuse:
            rd.shutdown()

'''
just delete user, not delete all user data

param user: str required user in cephfs
other params see connect function

return 0
'''
@login
def deluser(**kwargs):
    rd = kwargs.pop('rados')
    reuse = kwargs.get('reuse', False)
    try:
        user = kwargs.pop('user')
        if user == 'admin':
            msg = 'Unable to delete admin user'
            log.error(msg)
            raise DelUserError(msg)
        cmd = {'prefix':'auth del',
               'entity':'client.'+user,
               'format':'json'}
        ret, buf, out = rd.mon_command(json.dumps(cmd), '')
        if ret != 0 or 'updated' not in out:
            log.error('del user error: %s', out)
            raise DelUserError(out) 
        _set_quota(rd, user, 0, 'g', False)
        log.info('del user {0} successfully'.format(user))
        return 0
    finally:
        if not reuse:
            rd.shutdown()

'''
show current admin info

other params see connect function

return str
'''
@login
def show_info(**kwargs):
    configfile = kwargs.pop('configfile', default_admin_conf)
    cf = kwargs.pop('config')
    return os.path.abspath(configfile), str(cf)

import pytest
import os
import sys
import re
import json

sys.path.append('py-packages')

admin = pytest.importorskip('ceph_admin_interface')
prefix = '/pytestdir'
user = 'pytest_user888'

def test_lsuser_raise(capfd):
    with pytest.raises(admin.InvalidArgumentError) as err:
        admin.lsuser(configfile='notexistsfile')
    assert 'require ceph mon host' in err.value, err.value
    capfd.readouterr()

def test_lsuser_raise1(capfd):
    with pytest.raises(admin.InvalidArgumentError) as err:
        admin.lsuser(configfile='notexistsfile', cephaddr='a')
    assert 'require admin user key' in err.value, err.value
    capfd.readouterr()

def test_lsuser_raise2(capfd):
    with pytest.raises(admin.ConnectError) as err:
        admin.lsuser(configfile='notexistsfile', cephaddr='a', key='k')
    assert 'InvalidArgumentError' in str(err.value), str(err.value)
    capfd.readouterr()

def test_lsuser(capfd):
    admin.adduser(user=user, prefix=prefix)
    ret = admin.lsuser()
    assert isinstance(ret, list)
    assert user in ret
    admin.deluser(user=user, prefix=prefix)
    capfd.readouterr()

def test_getuser(capfd):
    with pytest.raises(admin.GetUserError) as err:
        admin.getuser(user=user)
    assert 'failed to find client' in err.value.message, err.value.message
    capfd.readouterr()

def test_adduser(capfd):
    key = admin.adduser(user=user, prefix=prefix)
    assert len(key)>0, key
    capfd.readouterr()
    key, groups, usage = admin.getuser(user=user, prefix=prefix)
    assert len(key)>0, key
    assert len(groups) == 0, groups
    assert usage[0][0] == '0', usage
    assert usage[0][1] == '0', usage
    admin.deluser(user=user, prefix=prefix)
    capfd.readouterr()

def test_adduser_group(capfd):
    key = admin.adduser(user=user,
            groups=['group1','group2','group3'],
            prefix=prefix,
            quota=1)
    assert len(key)>0, key
    admin.adduser(user='group2',
            prefix=prefix,
            quota=5)
    capfd.readouterr()
    key, groups, usage = admin.getuser(user=user, prefix=prefix)
    assert len(key)>0, key
    assert len(groups) == 3, groups
    assert groups[0] == 'group1', groups
    assert groups[1] == 'group2', groups
    assert groups[2] == 'group3', groups
    assert len(usage) == 4, usage
    assert usage[0][0] == '0', usage
    assert usage[0][1] == '0', usage
    assert usage[1][0] == '0', usage
    assert usage[1][1] == '5368709120', usage
    assert usage[2][0] == '0', usage
    assert usage[2][1] == '0', usage
    assert usage[3][0] == '0', usage
    assert usage[3][1] == '1073741824', usage
    admin.deluser(user=user, prefix=prefix)
    admin.deluser(user='group2', prefix=prefix)
    capfd.readouterr()

def test_adduser_group_path(capfd):
    key = admin.adduser(user=user,
            groups=['group1','group2'],
            prefix=prefix,
            paths=[os.path.join(prefix, 'mypath')],
            quota=1)
    assert len(key)>0, key
    capfd.readouterr()
    key, usage = admin.getuser(user=user, prefix=prefix, showpath=True)
    assert len(key)>0, key
    assert len(usage) == 4, usage
    assert ('0', '0', '%s/group1'%prefix) in usage, usage
    assert ('0', '0', '%s/group2'%prefix) in usage, usage
    assert ('0', '1073741824', '%s/mypath'%prefix) in usage, usage
    assert ('0', '1073741824', '%s/%s'%(prefix,user)) in usage, usage
    admin.deluser(user=user, prefix=prefix)
    capfd.readouterr()

def test_updateuser(capfd):
    key = admin.adduser(user=user, prefix=prefix)
    assert len(key)>0, key
    capfd.readouterr()
    assert 0 == admin.updateuser(user=user,
            groups=['group1','group2','group3'],
            prefix=prefix,
            quota=1)
    key, groups, usage = admin.getuser(user=user, prefix=prefix)
    assert len(key)>0, key
    assert len(groups) == 3, groups
    assert groups[0] == 'group1', groups
    assert groups[1] == 'group2', groups
    assert groups[2] == 'group3', groups
    assert len(usage) == 4, usage
    assert usage[0][0] == '0', usage
    assert usage[0][1] == '0', usage
    assert usage[1][0] == '0', usage
    assert usage[1][1] == '0', usage
    assert usage[2][0] == '0', usage
    assert usage[2][1] == '0', usage
    assert usage[3][0] == '0', usage
    assert usage[3][1] == '1073741824', usage
    admin.deluser(user=user, prefix=prefix)
    capfd.readouterr()

def test_updateuser_path(capfd):
    key = admin.adduser(user=user, prefix=prefix)
    assert len(key)>0, key
    capfd.readouterr()
    assert 0 == admin.updateuser(user=user,
            groups=['group1','group2'],
            paths=[os.path.join(prefix, 'mypath')],
            prefix=prefix,
            quota=1)
    key, usage = admin.getuser(user=user, prefix=prefix, showpath=True)
    assert len(key)>0, key
    assert len(usage) == 4, usage
    assert ('0', '0', '%s/group1'%prefix) in usage, usage
    assert ('0', '0', '%s/group2'%prefix) in usage, usage
    assert ('0', '1073741824', '%s/mypath'%prefix) in usage, usage
    assert ('0', '1073741824', '%s/%s'%(prefix,user)) in usage, usage
    admin.deluser(user=user, prefix=prefix)
    capfd.readouterr()

def test_updateuser_path_rm_add(capfd):
    key = admin.adduser(user=user,
            groups=['group1','group2'],
            prefix=prefix,
            paths=[os.path.join(prefix, 'mypath')],
            quota=1)
    assert len(key)>0, key
    capfd.readouterr()
    assert 0 == admin.updateuser(user=user,
            pathadd=[os.path.join(prefix, 'mypath1')],
            pathrm=[os.path.join(prefix, 'mypath')],
            quota=1)
    key, usage = admin.getuser(user=user, prefix=prefix, showpath=True)
    assert len(key)>0, key
    assert len(usage) == 4, usage
    assert ('0', '0', '%s/group1'%prefix) in usage, usage
    assert ('0', '0', '%s/group2'%prefix) in usage, usage
    assert ('0', '1073741824', '%s/mypath1'%prefix) in usage, usage
    assert ('0', '1073741824', '%s/%s'%(prefix,user)) in usage, usage
    admin.deluser(user=user, prefix=prefix)
    capfd.readouterr()

def test_getuser_usage(capfd):
    admin.adduser(user=user, quota=10, prefix=prefix)
    usage = admin.getuser_usage(user=user, prefix=prefix)
    capfd.readouterr()
    assert len(usage) >= 2, usage
    assert usage[0] == '0', usage
    assert usage[1] == '10737418240', usage
    admin.deluser(user=user, prefix=prefix)
    capfd.readouterr()

def test_get_all_users(capfd):
    us = admin.get_all_users()
    assert len(us) >= 0
    if us:
        assert us[0].get('user')
        assert us[0].get('used')
        assert us[0].get('quota')
    capfd.readouterr()

def test_exportuser(capfd):
    admin.adduser(user=user, prefix=prefix)
    ret = admin.exportuser(user=user, prefix=prefix)
    assert user in ret
    assert prefix in ret
    admin.deluser(user=user, prefix=prefix)
    capfd.readouterr()

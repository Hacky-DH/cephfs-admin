import pytest
import os
import sys

homedir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
pydir = os.path.join(homedir, 'py-packages')
sys.path.insert(1, pydir)
os.environ['CEPH_ADMIN_HOME'] = homedir
admin = pytest.importorskip('ceph_admin')

prog = 'ceph_admin_test'
prefix = '/pytestdir'
user = 'pytest_user886'

def adduser(capfd):
    sys.argv = [prog,'-vv','-x',prefix,'add',user]
    assert 0 == admin.main()
    out, err = capfd.readouterr()
    assert 'add user {0} successfully'.format(user) in out, out
    assert len(err) == 0, err
    
def deluser(capfd):
    sys.argv = [prog,'-vv','-x',prefix,'del',user]
    assert 0 == admin.main()
    capfd.readouterr()

@pytest.yield_fixture(scope='function')
def suit():
    yield

def test_zero(capsys):
    sys.argv = [prog]
    with pytest.raises(SystemExit) as err:
        admin.main()
    assert 2 == err.value.code
    _, err = capsys.readouterr()
    assert 'error: too few arguments' in err, err

def test_version(capsys):
    sys.argv = [prog,'-v']
    assert 0 == admin.main()
    out, _ = capsys.readouterr()
    assert out

def test_simple(suit, capfd):
    adduser(capfd)
    sys.argv = [prog,'-vv','-x',prefix,'get',user]
    assert 0 == admin.main()
    out, err = capfd.readouterr()
    assert user in out, out
    assert prefix in out, out
    assert len(err) == 0, err
    deluser(capfd)

def test_adduser_groups(suit, capfd):
    sys.argv = [prog,'-vv','-x',prefix,'add',user,
            '-g','test_g1','testg2','test_g3']
    assert 0 == admin.main()
    out, err = capfd.readouterr()
    assert 'add user {0} successfully'.format(user) in out, out
    assert len(err) == 0, err
    deluser(capfd)

def test_adduser_quota(suit, capfd):
    sys.argv = [prog,'-vv','-x',prefix,'add',user,
            '-g','test_g1','testg2','test_g3','-q','1']
    assert 0 == admin.main()
    out, err = capfd.readouterr()
    assert 'add user {0} successfully'.format(user) in out, out
    assert len(err) == 0, err
    deluser(capfd)

def test_updateuser_quota(suit, capfd):
    adduser(capfd)
    sys.argv = [prog,'-vv','-x',prefix,'update',user,
            '-g','test_g1','testg2','test_g3','-q','1']
    assert 0 == admin.main()
    out, err = capfd.readouterr()
    assert 'update user {0} successfully'.format(user) in out, out
    assert len(err) == 0, err
    deluser(capfd)

def test_exportuser(suit, capfd):
    adduser(capfd)
    sys.argv = [prog,'-vv','-x',prefix,'export',user]
    assert 0 == admin.main()
    out, err = capfd.readouterr()
    assert '"cephconf": null' in out, out
    assert len(err) == 0, err
    deluser(capfd)

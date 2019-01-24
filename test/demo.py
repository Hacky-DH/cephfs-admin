#demo for ceph_admin_interface
import sys
import os
sys.path.append('../py-packages')

import ceph_admin_interface as a
os.chdir('../conf')
user='testuser777'
cfg='admin_70.info'

rd = a.connect(configfile=cfg)
print(a.adduser(rados=rd, reuse=True, user=user, groups=['abc'], quato=2))
all = a.lsuser(rados=rd, reuse=True)
for u in all:
    print(u, a.getuser(rados=rd, reuse=True, user=u))
a.deluser(rados=rd, reuse=True, user=user)

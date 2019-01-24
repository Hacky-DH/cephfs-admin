#/bin/bash
set -e

prog='cephadmin'
py_version=$1
shift || true
version=$1
[ -z $version ] && version="0.0.1"

echo "$prog version $version $py_version"

cwd=$(cd $(dirname $0); pwd)
pushd $cwd > /dev/null

py.test -vsx $cwd/test

buildroot=tarball_build
/bin/rm -fr $cwd/$buildroot
rootdir=$cwd/$buildroot/$prog
mkdir -p $rootdir
/bin/cp -af bin conf lib install.sh $rootdir
mkdir -p $rootdir/py-packages
/bin/cp py-packages/*.py $rootdir/py-packages
if [[ $py_version = 'py34' ]];then
    /bin/cp py-packages/rados.cpython-34m.so $rootdir/py-packages/rados.so
    /bin/cp py-packages/cephfs.cpython-34m.so $rootdir/py-packages/cephfs.so
elif [[ $py_version = 'py36' ]];then
    /bin/cp py-packages/rados.cpython-36.so $rootdir/py-packages/rados.so
    /bin/cp py-packages/cephfs.cpython-36.so $rootdir/py-packages/cephfs.so
else
    /bin/cp py-packages/rados.so $rootdir/py-packages
    /bin/cp py-packages/cephfs.so $rootdir/py-packages
fi

pushd $rootdir > /dev/null
mkdir -p logs
sed -i "s#@CEPH_ADMIN_VERSION@#$version#" bin/cephadmin
/bin/chmod 0755 bin/* install.sh
/bin/rm conf/*.info

libs=(libceph-common.so.0 libcephfs.so.2.0.0 libibverbs.so.1.1.15 libnl-3.so.200.23.0 libnl-route-3.so.200.23.0 librados.so.2.0.0)
alibs=(libceph-common.so libcephfs.so.2 libibverbs.so.1 libnl-3.so.200 libnl-route-3.so.200 librados.so.2)
for(( i=0;i<${#libs[@]};i++)); do
    ln -srf lib/${libs[i]} lib/${alibs[i]}
done
popd > /dev/null

if [[ -z $py_version ]];then
    tarball="$cwd/$prog-$version.tar.gz"
else
    tarball="$cwd/$prog-$version-$py_version.tar.gz"
fi
pushd $buildroot > /dev/null
tar -czf $tarball $prog
popd > /dev/null
/bin/rm -fr $buildroot
# upload tarball
# sshpass -p $pwd scp -o StrictHostKeyChecking=no $tarball $url || true

echo "build $tarball done"

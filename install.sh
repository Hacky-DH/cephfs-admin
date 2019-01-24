#/bin/bash
set -e

cwd=$(cd $(dirname $0); pwd)
pushd $cwd > /dev/null

sed -i "s#@CEPH_ADMIN_HOME@#$cwd#" bin/cephadmin

if [[ $(whoami) == "root" ]];then
    ln -rsf bin/* /usr/bin/
else
    if [[ -d $HOME/.local/bin ]];then
        ln -rsf bin/* $HOME/.local/bin/
    else
        mkdir -p $HOME/bin
        ln -rsf bin/* $HOME/bin/
    fi
fi

#download config
wget http://ip/admin.info -qO conf/admin.info

echo "install cephadmin DONE"

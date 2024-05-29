Name        : KeyDB
Version     : 6.0.5
Release     : 1%{?dist}
Group       : Unspecified
License     : BSD
Packager    : Snap Inc.

URL         : https://keydb.dev
Summary     : A persistent key-value database

Requires:          /bin/awk
Requires:          logrotate
Requires(pre):     shadow-utils
Requires(post):    systemd
Requires(preun):   systemd
Requires(postun):  systemd
Requires(post):    chkconfig
Requires(preun):   chkconfig
Requires(preun):   initscripts
Requires(postun):  initscripts

# scripts

#preinstall scriptlet (using /bin/sh):
%pre
getent group keydb &> /dev/null || \
groupadd -r keydb &> /dev/null
getent passwd keydb &> /dev/null || \
useradd -r -g keydb -d /var/lib/keydb -s /sbin/nologin \
-c 'KeyDB Database Server' keydb &> /dev/null
exit 0

#postinstall scriptlet (using /bin/sh):
%post
if [ $1 -eq 1 ] ; then 
        # Initial installation 
        systemctl preset keydb.service >/dev/null 2>&1 || : 
fi 


if [ $1 -eq 1 ] ; then 
        # Initial installation 
        systemctl preset keydb-sentinel.service >/dev/null 2>&1 || : 
fi

chown -R keydb:keydb /etc/keydb
chown -R keydb:keydb /var/log/keydb
chown -R keydb:keydb /var/lib/keydb
chown -R keydb:keydb /usr/libexec/keydb-shutdown

#preuninstall scriptlet (using /bin/sh):
%preun
if [ $1 -eq 0 ] ; then 
        # Package removal, not upgrade 
        systemctl --no-reload disable keydb.service > /dev/null 2>&1 || : 
        systemctl stop keydb.service > /dev/null 2>&1 || : 
fi 


if [ $1 -eq 0 ] ; then 
        # Package removal, not upgrade 
        systemctl --no-reload disable keydb-sentinel.service > /dev/null 2>&1 || : 
        systemctl stop keydb-sentinel.service > /dev/null 2>&1 || : 
fi

#postuninstall scriptlet (using /bin/sh):
%postun
systemctl daemon-reload >/dev/null 2>&1 || : 
if [ $1 -ge 1 ] ; then 
        # Package upgrade, not uninstall 
        systemctl try-restart keydb.service >/dev/null 2>&1 || : 
fi 


systemctl daemon-reload >/dev/null 2>&1 || : 
if [ $1 -ge 1 ] ; then 
        # Package upgrade, not uninstall 
        systemctl try-restart keydb-sentinel.service >/dev/null 2>&1 || : 
fi


%Description
KeyDB is an advanced key-value store. It is often referred to as a data
structure server since keys can contain strings, hashes, lists, sets and
sorted sets.

You can run atomic operations on these types, like appending to a string;
incrementing the value in a hash; pushing to a list; computing set
intersection, union and difference; or getting the member with highest
ranking in a sorted set.

In order to achieve its outstanding performance, KeyDB works with an
in-memory dataset. Depending on your use case, you can persist it either
by dumping the dataset to disk every once in a while, or by appending
each command to a log.

KeyDB also supports typical master-replica setups, active replica setups
and multi master setups.

Other features include Transactions, Pub/Sub, Lua scripting, Keys with a
limited time-to-live, and configuration settings to make KeyDB behave like
a cache.

You can use KeyDB from most programming languages also.

%files
/etc/logrotate.d/keydb
/etc/systemd/system/keydb.service.d/limit.conf
/etc/systemd/system/keydb-sentinel.service.d/limit.conf
/usr/bin/*
/usr/lib/systemd/system/*
/usr/libexec/*
/usr/share/licenses/*
/usr/share/man/man1/*
/usr/share/man/man5/*
/var/lib/*
/var/log/*
/usr/lib64/redis/modules
%config(noreplace) /etc/keydb/*

#!/bin/bash
echo "10.13.9.17 lvdn001-priv.b2w" >> /etc/hosts 
echo "10.13.9.18 lvdn002-priv.b2w" >> /etc/hosts 
echo "10.13.9.19 lvdn003-priv.b2w" >> /etc/hosts
echo "10.13.9.22 lvdn004-priv.b2w" >> /etc/hosts
echo "10.13.9.23 lvdn005-priv.b2w" >> /etc/hosts
echo "10.13.9.24 lvdn006-priv.b2w" >> /etc/hosts
echo "10.13.9.25 lvdn007-priv.b2w" >> /etc/hosts
echo "10.13.9.35 lvdn008-priv.b2w" >> /etc/hosts
echo "10.13.9.36 lvdn009-priv.b2w" >> /etc/hosts
echo "10.13.9.37 lvdn010-priv.b2w" >> /etc/hosts
echo "10.13.9.38 lvdn011-priv.b2w" >> /etc/hosts
echo "10.13.9.20 lvnn-priv.b2w" >> /etc/hosts
echo "10.13.9.21 lvsb-priv.b2w" >> /etc/hosts
echo "10.13.9.27 lvwrk01-priv.b2w" >> /etc/hosts
echo "10.13.9.14 beth-2" >> /etc/hosts
echo "10.13.9.13 beth-1" >> /etc/hosts
echo "10.13.9.15 beth-3">> /etc/hosts
echo "10.13.9.20 lvnn-priv.b2w" >> /etc/hosts
echo "10.13.9.21 lvsb-priv.b2w" >> /etc/hosts
echo "10.13.9.27 lvwrk01-priv.b2w" >> /etc/hosts
/run.sh ${@}



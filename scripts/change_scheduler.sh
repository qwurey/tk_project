systemctl stop kube-scheduler

\cp ./tmp/new/kube-scheduler /usr/bin/ -f

systemctl start kube-scheduler

systemctl status kube-scheduler -l

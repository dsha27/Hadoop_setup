#!/usr/bin/python36
import subprocess as sp
import getpass 

print("\t\t\t\tWELCOME TO HADOOP SETUP:")
print("\t\t**************************************************")
print("\t\t**************************************************")

print("transferring jdk")
jdk=sp.getstatusoutput("scp /root/Desktop/jdk-8u171-linux-x64.rpm {}:/root/Desktop/jdk-8u171-linux-x64.rpm".format(ip))
print("transferring hadoop")
jdk=sp.getstatusoutput("scp /root/Desktop/hadoop-1.2.1-1.x86_64.rpm {}:/root/Desktop/hadoop-1.2.1-1.x86_64.rpm".format(ip))
print("installing jdk")
sp.getoutput("ssh {} rpm -ivh jdk-8u171-linux-x64.rpm".format(ip))
print("setting JAVA_HOME")
path1=sp.getstatusoutput("ssh {} 'echo export JAVA_HOME=/usr/java/jdk1.8.0_171-amd64/ >> /root/.bashrc'".format(ip))
sp.getstatusoutput("ssh {} 'export JAVA_HOME=/usr/java/jdk1.8.0_171-amd64/'".format(ip))
p=sp.getoutput("ssh {} date".format(ip))
print(p)
print("setting PATH")
path2 = sp.getstatusoutput("ssh {} 'export PATH=/usr/java/jdk1.8.0_171-amd64/bin:\$PATH'".format(ip))
sp.getstatusoutput("ssh {} 'echo export PATH=/usr/java/jdk1.8.0_171-amd64/bin:\$PATH >> /root/.bashrc'".format(ip))
p1=sp.getoutput("ssh {} date".format(ip))
print(p1)
print("installing hadoop")
o=sp.getstatusoutput("ssh {} rpm -ivh /root/Desktop/hadoop-1.2.1-1.x86_64.rpm --force".format(ip))
print(o[0])

print("""1.Setup master node
2.Setup slave node
3.Setup client node
4.Setup Hdfs Cluster
5.setup job tracker
6.setup task tracker	
      """)
ch = input("enter your choice:")

if int(ch)==1:
	m_ip = input("enter master_ip: ")
	master_hdfs="""
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>
<property>
<name>dfs.name.dir</name>
<value>/myname</value>
</property>
</configuration>
"""	
	
	h=open('hdfs','w')
	h.write(master_hdfs)
	h.close()
	sp.getoutput("scp /root/Desktop/PythonCode/hdfs {}:/etc/hadoop/hdfs-site.xml".format(m_ip))
	core_site="""<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>
<property>
<name>fs.default.name</name>
<value>hdfs://{}:9001</value>
</property>
</configuration>
""".format(m_ip)
	f=open('core','w')
	f.write(core_site)
	f.close()
	sp.getoutput("scp /root/Desktop/PythonCode/core {}:/etc/hadoop/core-site.xml".format(m_ip))
	sp.getoutput("ssh {} mkdir /myname".format(m_ip))
	sp.getoutput("ssh {} 'echo Y | hadoop namenode -format'".format(m_ip))
	sp.getoutput("ssh {} iptables -F".format(m_ip))
	sp.getoutput("ssh {} hadoop-daemon.sh start namenode".format(m_ip))
elif int(ch)==2:
	master_ip=input("enter master_ip:")
	n=int(input("enter no. of slaves to setup"))
	s_ip=[]
	for i in range(n):
		s_ip.append(input("enter slave_ip{}: ".format(i)))
	
		sp.getoutput("ssh {0} ssh-copy-id {0}".format(s_ip[i]))
		slave_hdfs="""<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>
<property>
<name>dfs.data.dir</name>
<value>/mydata</value>
</property>
</configuration>
"""
		s=open('slave-hdfs','w')
		s.write(slave_hdfs)
		s.close()	
		sp.getoutput("scp /root/Desktop/PythonCode/slave-hdfs {}:/etc/hadoop/hdfs-site.xml".format(s_ip[i]))

		core_site="""<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>
<property>
<name>fs.default.name</name>
<value>hdfs://{}:9001</value>
</property>
</configuration>
""".format(master_ip)
		sc=open('slave-core','w')
		sc.write(core_site)
		sc.close()
		sp.getoutput("scp /root/Desktop/PythonCode/slave-core {}:/etc/hadoop/core-site.xml".format(s_ip[i]))
		sp.getoutput("ssh {} mkdir /mydata".format(s_ip[i]))
		sp.getoutput("ssh {} iptables -F".format(s_ip[i]))
		sp.getoutput("ssh {} hadoop-daemon.sh start datanode".format(s_ip[i]))
elif int(ch)==3:
	c_ip=input("enter client_ip: ")
	master_ip=input("enter master_ip: ")
	sp.getoutput("ssh-copy-id {}".format(c_ip))
	core_site="""<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>
<property>
<name>fs.default.name</name>
<value>hdfs://{}:9001</value>
</property>
</configuration>
""".format(master_ip)
	cc=open('client-core','w')
	cc.write(core_site)
	cc.close()
	sp.getoutput("scp /root/Desktop/PythonCode/client-core {}:/etc/hadoop/core-site.xml".format(c_ip))
	sp.getoutput("ssh {} iptables -F".format(c_ip))
elif int(ch)==4:
	


elif int(ch)==5:
	master_ip=input("enter master_ip: ")
	job_ip=input("enter job trcakker ip: ")
	job_map="""<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>
<name>mapred.job.tracker</name>
<value>{}:9002</value>
</configuration>
""".format(job_map)
	jm=open('job-map','w')
	jm.write(job_map)
	jm.close()
	sp.getoutput("scp /root/Desktop/PyhtonCode/job-map {}:/etc/hadoop/mapred-site.xml".format(job_map))
	core_site="""<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>
<property>
<name>fs.default.name</name>
<value>hdfs://{}:9001</value>
</property>
</configuration>
""".format(master_ip)
	jc=open('core','w')
	jc.write(core_site)
	jc.close()
	sp.getoutput("scp /root/Desktop/PythonCode/core {}:/etc/hadoop/core-site.xml".format(job_ip))
	sp.getoutput("ssh {} iptables -F".format(job_ip))
	sp.getoutput("ssh {} hadoop-daemon.sh start jobtracker".format(job_ip))
elif int(ch)==6:
	job_ip=input("enter job tracker ip: ")
	n=int(input("enter no. of task tracker: "))
	for i in range(n):
		task_ip=input("enter task ip: ")
		
		task_map="""<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>
<name>mapred.job.tracker</name>
<value>{}:9002</value>
</configuration>
""".format(job_ip)
		jm=open('task-map','w')
		jm.write(task_map)
		jm.close()
		sp.getoutput("scp /root/Desktop/PyhtonCode/task-map {}:/etc/hadoop/mapred-site.xml".format(task_ip))
		sp.getoutput("ssh {} iptables -F".format(task_ip))
		sp.getoutput("ssh {} hadoop-daemon.sh start tasktracker".format(task_ip))
	client_ip=input("enter client_ip: ")
	client_map="""<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>
<name>mapred.job.tracker</name>
<value>{}:9002</value>
</configuration>
""".format(job_ip)
	jm=open('client-map','w')
	jm.write(client_map)
	jm.close()
	sp.getoutput("scp /root/Desktop/PyhtonCode/client-map {}:/etc/hadoop/mapred-site.xml".format(task_ip))	
else:
	print("error")

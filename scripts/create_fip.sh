counter=0
while [ $counter -lt 49 ]; do
	openstack floating ip create public
	let counter=counter+1
	done

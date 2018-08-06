import boto3

from providers import BaseProvider


class AWSProvider(BaseProvider):
    def __init__(self):
        self.boto_resource = boto3.resource('ec2')

    def create_vpc(self, cidr_block, tags):
        vpc = self.boto_resource.create_vpc(CidrBlock=cidr_block)
        vpc.create_tags(Tags=tags)
        vpc.wait_until_available()
        return vpc.id

    def create_internet_gateway(self, vpc_id, tags):
        gateway = self.boto_resource.create_internet_gateway()
        gateway.create_tags(Tags=tags)

        vpc = self.boto_resource.Vpc(vpc_id)
        vpc.attach_internet_gateway(
            InternetGatewayId=gateway.id,
            VpcId=vpc_id
        )
        return gateway.id

    def create_route_table(self, vpc_id, destination_cidr_block, gateway_id, tags):
        vpc = self.boto_resource.Vpc(vpc_id)
        route_table = vpc.create_route_table()
        route_table.create_tags(Tags=tags)

        route_table.create_route(
            DestinationCidrBlock=destination_cidr_block,
            GatewayId=gateway_id)
        return route_table.id

    def create_subnet(self, vpc_id, route_table_id, cidr_block, tags):
        subnet = self.boto_resource.create_subnet(CidrBlock=cidr_block,
                                                  VpcId=vpc_id)
        subnet.create_tags(Tags=tags)

        route_table = self.boto_resource.RouteTable(route_table_id)
        route_table.associate_with_subnet(SubnetId=subnet.id)

        return subnet.id

    def create_security_group(self, name, description, vpc_id, tags):
        sec_group = self.boto_resource.create_security_group(
            GroupName=name, Description=description, VpcId=vpc_id)
        sec_group.create_tags(Tags=tags)
        return sec_group.id

    def security_group_authorize_ingress(self, security_group_id, cidr_block, protocol, from_port, to_port):
        sec_group = self.boto_resource.SecurityGroup(security_group_id)
        sec_group.authorize_ingress(
            CidrIp=cidr_block,
            IpProtocol=protocol,
            FromPort=from_port,
            ToPort=to_port
        )
        return True

    def create_instance(self, image_id, vm_type, network_interfaces):
        instances = self.boto_resource.create_instances(
            ImageId=image_id,
            InstanceType=vm_type,
            MaxCount=1,
            MinCount=1,
            NetworkInterfaces=network_interfaces
        )
        instances[0].wait_until_running()
        return instances[0].id
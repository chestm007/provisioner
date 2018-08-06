class BaseProvider:
    def create_vpc(self, cidr_block, tags):
        pass

    def create_internet_gateway(self, vpc_id, tags):
        pass

    def create_security_group(self, name, description, vpc_id, tags):
        pass

    def security_group_authorize_ingress(self, security_group_id, cidr_block, protocol, from_port, to_port):
        pass

    def bind_security_group(self):
        pass

    def create_route_table(self, vpc_id, destination_cidr_block, gateway_id, tags):
        pass

    def create_subnet(self, vpc_id, route_table_id, cidr_block, tags):
        pass

    def create_disk(self):
        pass

    def attach_disk(self):
        pass

    def create_instance(self, image_id, vm_type, network_interfaces):
        pass

    def bind_ip(self):
        pass

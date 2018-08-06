import json
from abc import abstractmethod
from collections import namedtuple
from enum import Enum

from exceptions import ExecutionException


class TaskState(Enum):
    PENDING_PROVISION = 'PENDING_PROVISION'
    PROVISIONING = 'PROVISIONING'
    PROVISIONED = 'PROVISIONED'
    PENDING_DELETION = 'PENDING_DELETION'
    DELETING = 'DELETING'
    DELETED = 'DELETED'
    FAILED = 'FAILED'


class TaskAction(Enum):
    DELETE = 'DELETE'
    PROVISION = 'PROVISION'


ParentPayload = namedtuple('ParentPayload', 'type state payload')


class Task:
    def __init__(self, graph, task_id=None, task_state=None, payload=None):
        self.graph = graph
        self.id = task_id
        self._state = TaskState(task_state) if task_state else TaskState.PENDING_PROVISION
        self.payload = payload

    @abstractmethod
    def provision(self, cursor, provider):
        # if randint(0, 100) % 10 == 0:
        #     raise Exception("I FAILED")
        # time.sleep(randint(0, 1))
        # self.set_state(cursor, TaskState.PROVISIONED)
        pass

    @abstractmethod
    def delete(self, cursor, provider):
        pass

    def __call__(self, cursor, provider, action):
        try:
            if action == TaskAction.PROVISION:
                self.set_state(cursor, TaskState.PROVISIONING)
                if self.provision(cursor, provider):
                    self.set_state(cursor, TaskState.PROVISIONED)
                else:
                    self.set_state(cursor, TaskState.FAILED)
            elif action == TaskAction.DELETE:
                self.set_state(cursor, TaskState.DELETING)
                self.delete(cursor, provider)
            else:
                raise Exception("Invalid task action: {}".format(action))
            return self
        except Exception as e:
            self.set_state(cursor, TaskState.FAILED)
            raise ExecutionException(self, e)

    def persist(self, cursor, cluster_id, data_centre_id=None):
        if self.id is not None:
            raise Exception("This task is already persisted!")
        cursor.execute("""
            INSERT INTO node (type, payload, cluster, data_centre)
            VALUES (%(type)s, %(payload)s, %(cluster)s, %(data_centre)s) 
            RETURNING id
        """, dict(type=type(self).__name__,
                  payload=json.dumps(self.payload),
                  cluster=cluster_id,
                  data_centre=data_centre_id))
        self.id = cursor.fetchone().id
        return self

    def set_state(self, cursor, state):
        cursor.execute("""
            UPDATE node
            SET state = %(state)s
            WHERE id = %(id)s
        """, dict(state=state.name, id=self.id))
        self._state = state

    def set_payload(self, cursor, payload):
        cursor.execute("""
            UPDATE node
            SET payload = %(payload)s
            WHERE id = %(id)s
        """, dict(payload=json.dumps(payload), id=self.id))
        self.payload = payload

    @property
    def can_provision(self):
        if self._state != TaskState.PENDING_PROVISION:
            return False
        elif len(list(self.predecessors())) == 0:
            return True
        else:
            return all((s.state == TaskState.PROVISIONED for s in self.predecessors()))

    @property
    def can_delete(self):
        if self._state != TaskState.PENDING_DELETION:
            return False
        elif len(list(self.successors())) == 0:
            return True
        elif all((s.state == TaskState.DELETED for s in self.successors())):
            return True
        return False

    def successors(self):
        return self.graph.successors(self)

    def predecessors(self):
        return self.graph.predecessors(self)

    def retry_failed_provision(self):
        self._state = TaskState.PENDING_PROVISION

    @property
    def state(self):
        return self._state

    def _get_parents(self, cursor):
        cursor.execute("""
            SELECT node.type, node.state, node.payload
            FROM edge
                INNER JOIN node
                ON node.id = edge.from_node
            WHERE edge.to_node = %(id)s
        """, dict(id=self.id))
        return [ParentPayload(*r) for r in cursor]

    def _get_parent(self, cursor, task_type):
        cursor.execute("""
            SELECT node.type, node.state, node.payload
            FROM edge
                INNER JOIN node
                ON node.id = edge.from_node
            WHERE edge.to_node = %(id)s
            AND node.type = %(type)s
        """, dict(id=self.id, type=task_type))
        return ParentPayload(*cursor.fetchone())

    def __str__(self):
        return "{}-{}".format(type(self).__name__, id(self))

    def __repr__(self):
        return "{}(id='{}', state='{}')".format(type(self).__name__, self.id, self.state.name)


class DataCentre(Task):
    def provision(self, cursor, provider):
        print(self.payload)
        self.set_state(cursor, TaskState.PROVISIONED)
        return True

    def delete(self, cursor, provider):
        self.set_state(cursor, TaskState.DELETED)


class Cluster(Task):
    def provision(self, cursor, provider):
        self.set_state(cursor, TaskState.PROVISIONED)
        return True

    def delete(self, cursor, provider):
        self.set_state(cursor, TaskState.DELETED)


class Role(Task):
    def provision(self, cursor, provider):
        self.set_state(cursor, TaskState.PROVISIONED)
        return True


class VPC(Task):
    def provision(self, cursor, provider):
        vpc_id = provider.create_vpc(cidr_block='192.168.0.0/16', tags=[{"Key": "Name", "Value": "my new vpc bro"}])
        self.set_payload(cursor, dict(vpc_id=vpc_id))
        self.set_state(cursor, TaskState.PROVISIONED)
        return True


class BindSecurityGroup(Task):
    def provision(self, cursor, provider):
        self.set_state(cursor, TaskState.PROVISIONED)
        return True


class InternetGateway(Task):
    def provision(self, cursor, provider):
        parent = self._get_parent(cursor, 'VPC')
        vpc_id = parent.payload['vpc_id']
        gateway_id = provider.create_internet_gateway(vpc_id=vpc_id, tags=[
            {'Key': 'Name', 'Value': 'create an internet gateway bruh'}
        ])
        self.set_payload(cursor, dict(gateway_id=gateway_id, vpc_id=vpc_id))
        self.set_state(cursor, TaskState.PROVISIONED)
        return True


class RouteTable(Task):

    def provision(self, cursor, provider):
        parent = self._get_parent(cursor, 'InternetGateway')
        payload = parent.payload
        route_table_id = provider.create_route_table(vpc_id=payload['vpc_id'],
                                                     destination_cidr_block='0.0.0.0/0',
                                                     gateway_id=payload['gateway_id'],
                                                     tags=[{"Key": "Name", "Value": "yisss, route table"}])
        payload['route_table_id'] = route_table_id

        self.set_payload(cursor, payload)
        self.set_state(cursor, TaskState.PROVISIONED)
        return True


class SubNets(Task):
    def provision(self, cursor, provider):
        parent = self._get_parent(cursor, 'RouteTable')
        payload = parent.payload
        subnet_id = provider.create_subnet(vpc_id=payload['vpc_id'],
                                           route_table_id=payload['route_table_id'],
                                           cidr_block='192.168.1.0/24',
                                           tags=[{"Key": "Name", "Value": "mah subnet"}])

        payload['subnet_id'] = subnet_id
        self.set_payload(cursor, payload)
        self.set_state(cursor, TaskState.PROVISIONED)
        return True


class SecurityGroups(Task):
    def provision(self, cursor, provider):
        parent = self._get_parent(cursor, 'VPC')
        payload = parent.payload
        sec_group_id = provider.create_security_group(name='slice_0',
                                                      description='slice_0 sec group',
                                                      vpc_id=payload['vpc_id'],
                                                      tags=[{"Key": "Name", "Value": "mah security group"}])

        self.set_payload(cursor, dict(security_group_id=sec_group_id))
        provider.security_group_authorize_ingress(
            security_group_id=sec_group_id,
            cidr_block='0.0.0.0/0',
            protocol='icmp',
            from_port=-1,
            to_port=-1
        )
        self.set_state(cursor, TaskState.PROVISIONED)
        return True


class FirewallRules(Task):
    def provision(self, cursor, provider):
        self.set_state(cursor, TaskState.PROVISIONED)
        return True


class CreateEBS(Task):
    def provision(self, cursor, provider):
        self.set_state(cursor, TaskState.PROVISIONED)
        return True


class AttachEBS(Task):
    def provision(self, cursor, provider):
        self.set_state(cursor, TaskState.PROVISIONED)
        return True


class CreateInstance(Task):
    def provision(self, cursor, provider):
        instance_id = provider.create_instance(
            image_id='ami-25a8db1f', vm_type='t2.micro',
            network_interfaces=[{'SubnetId': 'subnet-52152035', 'DeviceIndex': 0, 'AssociatePublicIpAddress': True,
                                'Groups': ['sg-6ff64317']}])
        self.set_payload(cursor, dict(instance_id=instance_id))
        self.set_state(cursor, TaskState.PROVISIONED)
        return True


class BindIP(Task):
    def provision(self, cursor, provider):
        self.set_state(cursor, TaskState.PROVISIONED)
        return True

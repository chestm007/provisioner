import psycopg2.extras

from graph import AWSGraphBuilder
from provisioner_daemon import Provisioner

connection = psycopg2.connect(dbname='testgraphdb')
cursor = connection.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)

builder = AWSGraphBuilder(connection)
builder.create('test cluster', num_nodes=1, num_dcs=1, provider='BASE')
connection.commit()

provisioner = Provisioner(connection)
provisioner.run()

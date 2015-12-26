'''
Created on 24 mar. 2014

@author: david
'''
#!/usr/bin/env python
# -*- coding: utf-8 -*-
#!flask/bin/python

from sqlalchemy import *
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

db_name = "test_base"
db_user = "postgres"
db_host = "192.168.0.52"
db_pass = "postgres"
db_package = [db_name, db_user, db_host, db_pass]

dburl = 'postgresql+psycopg2://postgres:postgres@192.168.0.52/postgres'
dburl = "postgresql+psycopg2://" + str(db_user) + ":" + str(db_pass) + "@" + str(db_host) + "/" + str(db_name)
engine = create_engine(dburl) #, echo=True
db_session = scoped_session(sessionmaker(bind = engine))
psql_metadata = MetaData(schema = "public") #schema=active_schema
psql_metadata.reflect(bind = engine)
Base = declarative_base(metadata = psql_metadata)
Base.query = db_session.query_property()

def init_db():
    import web_service_db_mapping
    Base.metadata.create_all(bind=engine)

#session = Session()


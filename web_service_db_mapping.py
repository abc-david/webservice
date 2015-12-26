'''
Created on 24 mar. 2014

@author: david
'''
#!/usr/bin/env python
# -*- coding: utf-8 -*-
#!flask/bin/python

from sqlalchemy import *
from sqlalchemy.orm import *
from web_service_background import Base, db_session

class Mail(Base):
    __tablename__ = 'base'
    __table_args__ = {'extend_existing': True}
    id = Column('id', Integer, primary_key = True)
    mail = deferred(Column('mail', String, unique=True, nullable=False))
    domain = deferred(Column('domain', String, nullable=False))
    ok_npai = Column('ok_npai', Boolean, default=True)
    ok_fb = Column('ok_fb', Boolean, default=True)
    ok_syntax = Column('ok_syntax', Boolean, default=True)
    ok_mot = Column('ok_mot', Boolean, default=True)
    ok_plainte = Column('ok_plainte', Boolean, default=True)
    age = Column('age', Integer, default=1)
    multi = Column('multi', Integer, default=1)
    b2c = Column('b2c', Boolean, default=False)
    b2b = Column('b2b', Boolean, default=False)
    ouvr = Column('mimi_ouvr', Boolean, default=False)
    npai = Column('mimi_npai', Boolean, default=False)
    pcp = Column('pcp', Boolean, default=False)
    fab = Column('fab', Boolean, default=False)
    black = Column('black', Boolean, default=False)
    def __repr__(self):
        return '<Mail(%r, %r, multi: %r, ok_plainte: %r, npai: %r)>' % (self.id, self.mail, self.multi, self.ok_plainte, self.npai)

class Lead(Base):
    __tablename__ = 'lead'
    __table_args__ = {'extend_existing': True}
    id = Column('id', Integer, primary_key=True)
    mail_id = Column('mail_id', Integer, ForeignKey(Mail.id))
    ip = Column('ip', String)
    provenance = Column('provenance', String)
    date = Column('date', Date)
    mail = relationship("Mail", backref="lxm")
    def __repr__(self):
        return '<Lead(%r, %r, %r, %r)>' % (self.id, self.mail_id, self.ip, self.provenance)
    
class Id(Base):
    __tablename__ = 'id'
    __table_args__ = {'extend_existing': True}
    id = Column('id', Integer, primary_key=True)
    mail_id = Column('mail_id', Integer, ForeignKey(Mail.id))
    prenom = Column('prenom', String)
    nom = Column('nom', String)
    civilite = Column('civilite', Integer)
    birth = Column('birth', Date)
    cp = Column('cp', String)
    ville = Column('ville', String)
    mail = relationship("Mail", backref="info")
    def __repr__(self):
        return '<Id(%r, %r, %r, %r)>' % (self.id, self.mail_id, self.prenom, self.nom)

class Md5(Base):
    __tablename__ = 'md5'
    __table_args__ = {'extend_existing': True}
    id = Column('id', Integer, primary_key=True)
    mail_id = Column('mail_id', Integer, ForeignKey(Mail.id))
    md5 = Column('md5', String)
    mail = relationship("Mail", backref="md5")
    def __repr__(self):
        return '<Md5(%r, %r, %r)>' % (self.id, self.mail_id, self.md5)
    
def assess_query_result(query_result, list_position = ""):
    if query_result:
        if isinstance(query_result, list):
            if len(query_result) > 0:
                if list_position == "":
                    list_position = 0
                return [True, query_result[list_position]]
            else:
                return [True, query_result[0]]
        else:
            return [True, query_result]
    else:
        return [False, "Query failed. No records returned"]

def md5_2_mail(md5):
    if isinstance(md5, basestring):
        if len(md5) == 32:
            try:
                md5_query_result = db_session().query(Md5).filter(Md5.md5 == md5).all()
            except:
                return [False, "Pb. with DB connection or mapping"]
            md5_result = assess_query_result(md5_query_result)
            if md5_result[0]:
                my_Md5 = md5_result[1]
                try:
                    mail_result = assess_query_result(my_Md5.mail)
                except:
                    return [False, "'md5' argument found in DB, but impossible to associate it with an existing Mail."]
                if mail_result[0]:
                    return [True, mail_result[1]]
                else:
                    return [False, "'md5' argument found in DB. But Mail query returned : " + mail_result[1]]
            else:
                return[False, md5_result[1]]
        else:
            return [False, "Argument passed as 'md5' is not valid. Needs to be 32 characters long."]
    else:
        return [False, "Argument passed as 'md5' is not 'basestring' type."]

def return_md5_request(md5):
    mail_query = md5_2_mail(md5)
    if mail_query[0]:
        Mail = mail_query[1]
        return_dict = {'mail' : Mail.mail.encode('utf-8')}
        info_result = assess_query_result(Mail.info)
        if info_result[0]:
            Info = info_result[1]
            if Info.prenom:
                return_dict['prenom'] = Info.prenom.encode('utf-8')
            if Info.nom:
                return_dict['nom'] = Info.nom.encode('utf-8')
            if Info.cp:
                return_dict['cp'] = Info.cp.encode('utf-8')
            if Info.ville:
                return_dict['ville'] = Info.ville.encode('utf-8')
        return {md5 : return_dict}
    else:
        return {md5 : {'error' : str(mail_query[1])}}
from sqlalchemy import Column, Integer, String, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()

class Ticket(Base):
    __tablename__ = 'sw_tickets'
    ticketid = Column(Integer, primary_key=True)
    subject = Column(String(255))
    ticketstatustitle = Column(String(255))
    dateline = Column(Integer)  # Unix timestamp for created
    lastactivity = Column(Integer)  # Unix timestamp for last activity
    resolutiondateline = Column(Integer)  # Unix timestamp for closed
    duedate = Column(Integer)  # Unix timestamp for service_date
    totalreplies = Column(Integer)
    lastpostid = Column(Integer)
    tickettypetitle = Column(String(255))
    departmenttitle = Column(String(255))
    fullname = Column(String(255))
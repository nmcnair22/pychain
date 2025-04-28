from sqlalchemy import Column, Integer, String, SmallInteger, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.sqltypes import Text

Base = declarative_base()

class Ticket(Base):
    __tablename__ = 'sw_tickets'
    ticketid = Column(Integer, primary_key=True)
    subject = Column(String(255))
    ticketstatustitle = Column(String(255))
    dateline = Column(Integer)  # Unix timestamp for created
    lastactivity = Column(Integer)  # Unix timestamp for last activity
    totalreplies = Column(Integer)
    locationid = Column(Integer)
    tickettypeid = Column(Integer)
    departmentid = Column(Integer)
    posts = relationship("Posts", back_populates="ticket")
    notes = relationship("Notes", back_populates="ticket")

class Posts(Base):
    __tablename__ = 'sw_ticketposts'
    ticketpostid = Column(Integer, primary_key=True)
    ticketid = Column(Integer, ForeignKey('sw_tickets.ticketid'))
    dateline = Column(Integer)  # Unix timestamp
    fullname = Column(String(255))
    contents = Column(Text)
    isprivate = Column(SmallInteger)
    ticket = relationship("Ticket", back_populates="posts")

class Notes(Base):
    __tablename__ = 'sw_ticketnotes'
    ticketnoteid = Column(Integer, primary_key=True)
    linktypeid = Column(Integer)  # Maps to ticketid in raw data
    dateline = Column(Integer)  # Unix timestamp
    staffname = Column(String(255))
    note = Column(Text)
    ticket = relationship("Ticket", back_populates="notes")
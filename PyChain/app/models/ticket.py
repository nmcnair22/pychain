from sqlalchemy import Column, String, DateTime, JSON, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Ticket(Base):
    __tablename__ = 'tickets'
    ticketid = Column(String, primary_key=True)
    subject = Column(String)
    status = Column(String)
    created = Column(DateTime)
    closed = Column(DateTime)
    service_date = Column(DateTime)
    site_number = Column(String)
    customer = Column(String)
    state = Column(String)
    city = Column(String)
    location_name = Column(String)
    location_id = Column(String)
    project_id = Column(String)
    linked_tickets = Column(JSON)
    total_replies = Column(String)
    posts = relationship("Posts", back_populates="ticket")
    notes = relationship("Notes", back_populates="ticket")

class Posts(Base):
    __tablename__ = 'posts'
    ticketpostid = Column(String, primary_key=True)
    ticketid = Column(String, ForeignKey('tickets.ticketid'))
    post_dateline = Column(DateTime)
    fullname = Column(String)
    contents = Column(String)
    isprivate = Column(String)
    ticket = relationship("Ticket", back_populates="posts")

class Notes(Base):
    __tablename__ = 'notes'
    ticketnoteid = Column(String, primary_key=True)
    ticketid = Column(String, ForeignKey('tickets.ticketid'))
    note_dateline = Column(DateTime)
    staffname = Column(String)
    note = Column(String)
    ticket = relationship("Ticket", back_populates="notes")
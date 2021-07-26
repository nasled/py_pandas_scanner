from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy import create_engine, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

main_table = 'transactions'
database_file = 'data/database.sqlite'

engine = create_engine('sqlite:///' + database_file, echo=True)
Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()
class Transaction(Base):
    __tablename__ = main_table

    transaction = Column(String, primary_key=True)
    timestamp = Column(DateTime)
    amount = Column(Integer)
    sender = Column(String)
    receiver = Column(String)

Base.metadata.create_all(engine)

inspector = inspect(engine)
if Transaction.__tablename__ in inspector.get_table_names():
    print('Database has been created.')
else:
    print('Database has not been created yet.')

from sqlalchemy.orm import Session
from app.models.user import User

class UserService:
    """Service to handle user operations from CISSDM database"""
    
    @staticmethod
    def get_all_users(db: Session, skip: int = 0, limit: int = 100):
        """Get all users from the database"""
        return db.query(User).offset(skip).limit(limit).all()
    
    @staticmethod
    def get_user_by_id(db: Session, user_id: int):
        """Get a specific user by ID"""
        return db.query(User).filter(User.id == user_id).first()
    
    @staticmethod
    def get_user_by_username(db: Session, username: str):
        """Get a specific user by username"""
        return db.query(User).filter(User.username == username).first()
    
    @staticmethod
    def create_user(db: Session, user_data: dict):
        """Create a new user (for in-memory database testing only)"""
        user = User(**user_data)
        db.add(user)
        db.commit()
        db.refresh(user)
        return user 
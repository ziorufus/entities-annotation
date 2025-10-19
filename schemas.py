from pydantic import BaseModel

class UpdateInfoRequest(BaseModel):
    id: int
    location_info: dict

class UpdateGroupRequest(BaseModel):
    id1: int
    id2: int

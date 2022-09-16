from pydantic import BaseModel


class RewardStatusDataSchema(BaseModel):
    new_status: str
    original_status: str | None
    reward_slug: str
    pending_reward_id: str | None

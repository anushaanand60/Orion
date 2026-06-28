from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision='0929abe8f5b4'
down_revision='7a7f5da43e0b'
branch_labels=None
depends_on=None

def upgrade() -> None:
    op.add_column('tasks', sa.Column('retry_count', sa.Integer(), server_default='0', nullable=False))
    op.add_column('tasks', sa.Column('max_retries', sa.Integer(), server_default='3', nullable=False))

def downgrade() -> None:
    op.drop_column('tasks', 'max_retries')
    op.drop_column('tasks', 'retry_count')

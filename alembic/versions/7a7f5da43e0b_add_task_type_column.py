from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision='7a7f5da43e0b'
down_revision='76523dd7f503'
branch_labels=None
depends_on=None

def upgrade() -> None:
    op.add_column('tasks', sa.Column('task_type', sa.String(), nullable=False, server_default='echo'))
    op.alter_column('tasks', 'task_type', server_default=None)

def downgrade() -> None:
    op.drop_column('tasks', 'task_type')

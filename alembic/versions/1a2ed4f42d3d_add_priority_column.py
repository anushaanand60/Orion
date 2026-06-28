from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision='1a2ed4f42d3d'
down_revision='0929abe8f5b4'
branch_labels=None
depends_on=None

def upgrade() -> None:
    op.add_column('tasks', sa.Column('priority', sa.Enum('HIGH', 'DEFAULT', 'LOW', name='taskpriority', native_enum=False), server_default='default', nullable=False))

def downgrade() -> None:
    op.drop_column('tasks', 'priority')

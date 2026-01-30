import asyncio
import logging
from datetime import datetime
from sqlalchemy import select, update
from app.infrastructure.celery.app import celery_app
from app.infrastructure.database.factory import get_db
from app.domains.repo_mgmt.models.repository import RepoRecord, ProcessingStatus
from app.domains.repo_mgmt.services.remote_git_service import RemoteGitService


@celery_app.task(bind=True)
def clone_repository_task(self, repo_id: str):
    """异步克隆仓库任务"""
    
    async def _clone_repository():
        async for session in get_db():
            try:                                
                # 获取仓库信息
                result = await session.execute(
                    select(RepoRecord).where(RepoRecord.id == repo_id)
                )
                repo_record = result.scalar_one_or_none()
                if not repo_record:
                    raise Exception(f"仓库 {repo_id} 不存在")

                # 更新状态为克隆中
                await session.execute(
                    update(RepoRecord)
                    .where(RepoRecord.id == repo_id)
                    .values(
                        processing_status=ProcessingStatus.CLONING,
                        processing_progress=10,
                        processing_message="开始克隆仓库",
                        updated_at=datetime.utcnow()
                    )
                )
                await session.commit()
                
                # 执行克隆操作（同步等待完成）
                git_info = await RemoteGitService.clone_repository(
                    session=session,
                    repository_url=repo_record.repo_url,
                    local_repo_path=repo_record.local_path,
                    branch=repo_record.repo_branch,
                    user_id=repo_record.create_user_id
                )
                
                # 克隆成功，更新仓库信息
                await session.execute(
                    update(RepoRecord)
                    .where(RepoRecord.id == repo_id)
                    .values(
                        version=git_info.version,
                        processing_status=ProcessingStatus.COMPLETED,
                        processing_progress=100,
                        processing_message="仓库克隆完成",
                        is_cloned=True,
                        updated_at=datetime.utcnow()
                    )
                )
                await session.commit()
                logging.info(f"仓库 {repo_record.repo_name} 克隆完成")
                
            except Exception as e:
                # 更新状态为失败
                await session.execute(
                    update(RepoRecord)
                    .where(RepoRecord.id == repo_id)
                    .values(
                        processing_status=ProcessingStatus.FAILED,
                        processing_progress=0,
                        processing_message="克隆失败",
                        processing_error=str(e),
                        updated_at=datetime.utcnow()
                    )
                )
                await session.commit()
                logging.error(f"仓库 {repo_id} 克隆失败: {e}")
                raise
    
    # 运行异步任务
    asyncio.run(_clone_repository())

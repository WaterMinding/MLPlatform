# 导入标准库模块
import asyncio

# FIFO锁类
class FIFOLock:
    def __init__(self):

        self._locked = False

        self._waiters = asyncio.Queue()

    # 获取锁方法 
    async def acquire(self):
        
        if not self._locked:
            self._locked = True
            return True
        
        else:
            
            # 将任务放入等待队列
            future = asyncio.Future()
            await self._waiters.put(future)
            
            # 等待锁被分配
            await future
            return True

    # 释放锁方法
    def release(self):
        
        # 如果没有任务在等待，则将锁状态设置为未锁定
        if self._waiters.empty():
            self._locked = False
        
        # 如果有任务在等待，则分配锁给下一个任务
        else:
            
            # 从队列中取出下一个任务并分配锁
            # (如果任务被取消或已经结束则不分配)
            future = self._waiters.get_nowait()
            if not future.done():
                future.set_result(True)

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.release()


# 数据池文件锁
pool_lock = FIFOLock()
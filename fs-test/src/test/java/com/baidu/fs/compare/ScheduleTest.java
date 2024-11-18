package com.baidu.fs.compare;

import java.sql.SQLException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 如果在线程池里有未捕获的异常，则周期性调度停止执行。
 * 如以下仅打印 “i==0”。如果把 NullPointerException 注释掉，则打印全部。
 */
public class ScheduleTest {
    static int i = 0;
    public static void main(String[] args) {
        ScheduledThreadPoolExecutor scheduledThreadPoolExecutor =
                new ScheduledThreadPoolExecutor(1);
        scheduledThreadPoolExecutor.scheduleAtFixedRate(() -> {
            System.out.println("i==" + i);
            try {
                if (i==0) {
                    i++;
                    throw new NullPointerException();
                } else {
                    i++;
                    throw new SQLException("");
                }
            } catch (SQLException e)  {
                // e.printStackTrace();
            }

        }, 1, 1, TimeUnit.SECONDS);

        try {
            TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        scheduledThreadPoolExecutor.shutdownNow();
    }
}

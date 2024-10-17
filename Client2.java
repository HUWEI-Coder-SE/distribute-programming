package q2;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class Client2 {
    private static final String SERVER_ADDRESS = "localhost";
    private static final int PORT = 12345;
    private static final String FILE_NAME = "2153393-hw2-q1.dat";

    public static void main(String[] args) {
        try {
            // 创建与服务器的连接
            Socket socket = new Socket(SERVER_ADDRESS, PORT);
            System.out.println("客户端2已连接到服务器。");

            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
            oos.flush();
            ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());

            // 请求读取权限
            System.out.println("客户端2请求读取权限。");
            oos.writeObject("read");
            String response = (String) ois.readObject();
            if ("read_granted".equals(response)) {
                System.out.println("客户端2获得读取权限，开始查找数据...");
                // 执行查找任务
                searchAndDelete();
            }

            // 请求写入权限
            System.out.println("客户端2请求写入权限。");
            oos.writeObject("write");
            response = (String) ois.readObject();
            if ("write_granted".equals(response)) {
                System.out.println("客户端2获得写入权限，开始删除和更新数据...");
                // 执行删除和写入操作
                performDeletionAndUpdate();

                // 通知服务器写入完成
                oos.writeObject("write_complete");
                System.out.println("客户端2已完成写入操作，通知服务器。");
            }

            // 请求退出
            oos.writeObject("exit");
            System.out.println("客户端2发送退出请求。");

            // 关闭连接
            ois.close();
            oos.close();
            socket.close();
            System.out.println("客户端2已关闭连接。");

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    // 查找不小于且最接近 1024 * 64 的整数
    private static void searchAndDelete() {
        try (RandomAccessFile raf = new RandomAccessFile(FILE_NAME, "r")) {
            long[] header = readHeader(raf);
            long startPos = header[2];
            long length = header[3];

            System.out.println("客户端2读取的文件头信息：");
            System.out.println("Part B 起始位置：" + startPos + "，长度：" + length);

            long startTime = System.currentTimeMillis(); // 开始计时

            int targetValue = 1024 * 64; // 65,536
            long totalIntegers = length / 4;

            System.out.println("客户端2开始随机查找（使用二分查找）...");
            // 二分查找第一个大于等于目标值的位置
            long left = 0;
            long right = totalIntegers - 1;
            long foundIndex = -1;

            while (left <= right) {
                long mid = (left + right) / 2;
                raf.seek(startPos + mid * 4);
                int value = raf.readInt();

                // 调试信息：当前检查的位置和值
                System.out.println("检查位置：" + (startPos + mid * 4) + "，值：" + value);

                if (value >= targetValue) {
                    foundIndex = mid;
                    right = mid - 1;
                } else {
                    left = mid + 1;
                }
            }

            int closestValue;
            List<Long> positions = new ArrayList<>();

            if (foundIndex != -1) {
                raf.seek(startPos + foundIndex * 4);
                closestValue = raf.readInt();
                positions.add(startPos + foundIndex * 4);

                // 向后查找相同的值
                long index = foundIndex + 1;
                while (index < totalIntegers) {
                    raf.seek(startPos + index * 4);
                    int value = raf.readInt();
                    if (value == closestValue) {
                        positions.add(startPos + index * 4);
                        index++;
                    } else {
                        break;
                    }
                }
            } else {
                closestValue = Integer.MAX_VALUE;
            }

            long endTime = System.currentTimeMillis(); // 结束计时

            // 输出结果
            if (closestValue != Integer.MAX_VALUE) {
                System.out.println("查找完成，耗时：" + (endTime - startTime) + " 毫秒。");
                System.out.println("找到的整数值：" + closestValue);
                System.out.println("对应的指针位置：");
                for (long pos : positions) {
                    System.out.println("    位置：" + pos);
                }
            } else {
                System.out.println("未找到大于等于目标值的整数。");
            }

            // 保存查找结果，供后续删除使用
            ClientData.setClosestValue(closestValue);
            ClientData.setPositions(positions);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 执行删除和更新操作
    private static void performDeletionAndUpdate() {
        try (RandomAccessFile raf = new RandomAccessFile(FILE_NAME, "rw")) {
            long[] header = readHeader(raf);
            long startPos = header[2];
            long length = header[3];

            int valueToDelete = ClientData.getClosestValue();
            List<Long> positions = ClientData.getPositions();

            if (valueToDelete == Integer.MAX_VALUE || positions.isEmpty()) {
                System.out.println("没有需要删除的整数。");
                return;
            }

            long startTime = System.currentTimeMillis(); // 开始计时

            // 删除并移动数据
            deleteIntegers(raf, startPos, length, valueToDelete, positions);

            long endTime = System.currentTimeMillis(); // 结束计时

            // 更新文件头信息
            long newLength = length - positions.size() * 4;
            long shiftAmount = positions.size() * 4;
            header[3] = newLength;
            header[4] -= shiftAmount;
            writeHeader(raf, header);

            // 打印新的文件头信息
            System.out.println("更新后的文件头信息：");
            System.out.println("Part A 起始位置：" + header[0] + "，长度：" + header[1]);
            System.out.println("Part B 起始位置：" + header[2] + "，长度：" + header[3]);
            System.out.println("Part C 起始位置：" + header[4] + "，长度：" + header[5]);

            System.out.println("删除和更新操作完成，耗时：" + (endTime - startTime) + " 毫秒。");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 删除整数并移动数据
    private static void deleteIntegers(RandomAccessFile raf, long startPos, long length, int valueToDelete, List<Long> positions) throws IOException {
        long deleteStartIndex = (positions.get(0) - startPos) / 4;
        long deleteCount = positions.size();
        long readPos = startPos + (deleteStartIndex + deleteCount) * 4;
        long writePos = startPos + deleteStartIndex * 4;
        long endPos = startPos + length;

        byte[] buffer = new byte[4 * 1024]; // 4KB缓冲区

        while (readPos < endPos) {
            int bytesToRead = (int) Math.min(buffer.length, endPos - readPos);
            raf.seek(readPos);
            raf.readFully(buffer, 0, bytesToRead);

            raf.seek(writePos);
            raf.write(buffer, 0, bytesToRead);

            readPos += bytesToRead;
            writePos += bytesToRead;
        }

        // 截断文件
        long oldLength = raf.length();
        raf.setLength(oldLength - positions.size() * 4);
    }

    // 读取文件头信息
    private static long[] readHeader(RandomAccessFile raf) throws IOException {
        raf.seek(0);
        long[] header = new long[6];
        for (int i = 0; i < 6; i++) {
            header[i] = raf.readLong();
        }
        return header;
    }

    // 写入文件头信息
    private static void writeHeader(RandomAccessFile raf, long[] header) throws IOException {
        raf.seek(0);
        for (long value : header) {
            raf.writeLong(value);
        }
    }
}


package q2;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class Client1 {
    private static final String SERVER_ADDRESS = "localhost";
    private static final int PORT = 12345;
    private static final String FILE_NAME = "2153393-hw2-q1.dat";

    public static void main(String[] args) {
        try {
            // 创建与服务器的连接
            Socket socket = new Socket(SERVER_ADDRESS, PORT);
            System.out.println("客户端1已连接到服务器。");

            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
            oos.flush(); // 确保对象输出流的头信息发送
            ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());

            // 请求读取权限
            System.out.println("客户端1请求读取权限。");
            oos.writeObject("read");
            String response = (String) ois.readObject();
            if ("read_granted".equals(response)) {
                System.out.println("客户端1获得读取权限，开始查找数据...");
                // 执行查找任务
                searchAndDelete();
            }

            // 请求写入权限
            System.out.println("客户端1请求写入权限。");
            oos.writeObject("write");
            response = (String) ois.readObject();
            if ("write_granted".equals(response)) {
                System.out.println("客户端1获得写入权限，开始删除和更新数据...");
                // 执行删除和写入操作
                performDeletionAndUpdate();

                // 通知服务器写入完成
                oos.writeObject("write_complete");
                System.out.println("客户端1已完成写入操作，通知服务器。");
            }

            // 请求退出
            oos.writeObject("exit");
            System.out.println("客户端1发送退出请求。");

            // 关闭连接
            ois.close();
            oos.close();
            socket.close();
            System.out.println("客户端1已关闭连接。");

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    // 查找不小于且最接近 1024 * 64 的整数
    private static void searchAndDelete() {
        try (RandomAccessFile raf = new RandomAccessFile(FILE_NAME, "r")) {
            long[] header = readHeader(raf);
            long startPos = header[0];
            long length = header[1];

            System.out.println("客户端1读取的文件头信息：");
            System.out.println("Part A 起始位置：" + startPos + "，长度：" + length);

            raf.seek(startPos);

            long startTime = System.currentTimeMillis(); // 开始计时

            int batchSize = 1_000_000; // 每次读取100万整数
            byte[] buffer = new byte[batchSize * 4];
            int targetValue = 1024 * 64; // 65,536
            int closestValue = Integer.MAX_VALUE;
            List<Long> positions = new ArrayList<>();

            long totalIntegers = length / 4;
            long readIntegers = 0;
            long position = startPos;

            System.out.println("客户端1开始顺序查找...");
            while (readIntegers < totalIntegers) {
                int integersToRead = (int) Math.min(batchSize, totalIntegers - readIntegers);
                raf.readFully(buffer, 0, integersToRead * 4);
                for (int i = 0; i < integersToRead; i++) {
                    int offset = i * 4;
                    int value = ((buffer[offset] & 0xFF) << 24) |
                            ((buffer[offset + 1] & 0xFF) << 16) |
                            ((buffer[offset + 2] & 0xFF) << 8) |
                            (buffer[offset + 3] & 0xFF);

                    if (value >= targetValue) {
                        if (value < closestValue) {
                            closestValue = value;
                            positions.clear();
                            positions.add(position + (readIntegers + i) * 4);
                        } else if (value == closestValue) {
                            positions.add(position + (readIntegers + i) * 4);
                        }
                    }
                }
                readIntegers += integersToRead;

                // 调试信息：已读取的整数数量
                if (readIntegers % (10_000_000) == 0) {
                    System.out.println("客户端1已读取整数数量：" + readIntegers);
                }
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
            long startPos = header[0];
            long length = header[1];

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
            header[1] = newLength;
            header[2] -= shiftAmount;
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
        raf.seek(startPos);
        long readPos = startPos;
        long writePos = startPos;

        long endPos = startPos + length;

        byte[] buffer = new byte[4 * 1024]; // 4KB缓冲区

        long totalIntegers = length / 4;
        long currentIntegerIndex = 0;

        while (readPos < endPos) {
            int bytesToRead = (int) Math.min(buffer.length, endPos - readPos);
            raf.seek(readPos);
            raf.readFully(buffer, 0, bytesToRead);

            int validDataLength = 0;
            for (int i = 0; i < bytesToRead; i += 4) {
                int value = ((buffer[i] & 0xFF) << 24) |
                        ((buffer[i + 1] & 0xFF) << 16) |
                        ((buffer[i + 2] & 0xFF) << 8) |
                        (buffer[i + 3] & 0xFF);

                if (value != valueToDelete) {
                    // 将该整数写入到写入位置
                    raf.seek(writePos + validDataLength);
                    raf.writeInt(value);
                    validDataLength += 4;
                }
                currentIntegerIndex++;
            }
            readPos += bytesToRead;
            writePos += validDataLength;
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

// 辅助类，用于保存客户端的数据
class ClientData {
    private static int closestValue;
    private static List<Long> positions;

    public static int getClosestValue() {
        return closestValue;
    }

    public static void setClosestValue(int value) {
        closestValue = value;
    }

    public static List<Long> getPositions() {
        return positions;
    }

    public static void setPositions(List<Long> pos) {
        positions = pos;
    }
}

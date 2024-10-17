package q2;

import mypackage.HuffmanNode;

import java.io.*;
import java.net.Socket;
import java.util.*;

public class Client3 {
    private static final String SERVER_ADDRESS = "localhost";
    private static final int PORT = 12345;
    private static final String FILE_NAME = "2153393-hw2-q1.dat";
    private static final String HUFFMAN_TREE_FILE = "huffman_tree.dat";
    private static final int MAX_VALUE = 1024 * 128; // 131,072

    public static void main(String[] args) {
        try {
            // 创建与服务器的连接
            Socket socket = new Socket(SERVER_ADDRESS, PORT);
            System.out.println("客户端3已连接到服务器。");

            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
            oos.flush(); // 确保对象输出流的头信息发送
            ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());

            // 请求读取权限
            System.out.println("客户端3请求读取权限。");
            oos.writeObject("read");
            String response = (String) ois.readObject();
            if ("read_granted".equals(response)) {
                System.out.println("客户端3获得读取权限，开始查找数据...");
                // 执行查找任务
                searchAndDelete();
            }

            // 请求写入权限
            System.out.println("客户端3请求写入权限。");
            oos.writeObject("write");
            response = (String) ois.readObject();
            if ("write_granted".equals(response)) {
                System.out.println("客户端3获得写入权限，开始删除和更新数据...");
                // 执行删除和写入操作
                performDeletionAndUpdate();

                // 通知服务器写入完成
                oos.writeObject("write_complete");
                System.out.println("客户端3已完成写入操作，通知服务器。");
            }

            // 请求退出
            oos.writeObject("exit");
            System.out.println("客户端3发送退出请求。");

            // 关闭连接
            ois.close();
            oos.close();
            socket.close();
            System.out.println("客户端3已关闭连接。");

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    // 查找不小于且最接近 1024 * 64 的整数
    private static void searchAndDelete() {
        try (RandomAccessFile raf = new RandomAccessFile(FILE_NAME, "r")) {
            long[] header = readHeader(raf);
            long startPos = header[4];
            long length = header[5];

            System.out.println("客户端3读取的文件头信息：");
            System.out.println("Part C 起始位置：" + startPos + "，长度：" + length);

            // 检查是否越界
            long fileLength = raf.length();
            if (startPos + length > fileLength) {
                System.out.println("错误：要读取的数据超过文件长度。文件长度：" + fileLength);
                return;
            }

            // 加载霍夫曼树
            HuffmanNode root = loadHuffmanTree();
            if (root == null) {
                System.out.println("无法加载霍夫曼树，无法解压缩数据。");
                return;
            }

            raf.seek(startPos);

            long startTime = System.currentTimeMillis(); // 开始计时

            // 将压缩的数据读取到字节数组
            byte[] compressedData = new byte[(int) length];
            raf.readFully(compressedData);

            System.out.println("客户端3开始解码压缩数据...");

            // 使用BitInputStream逐位读取数据
            BitInputStream bis = new BitInputStream(compressedData);

            int targetValue = 1024 * 64; // 65,536
            int closestValue = Integer.MAX_VALUE;
            List<Long> positions = new ArrayList<>(); // 存储指针位置（文件偏移量）

            HuffmanNode currentNode = root;
            long bitPosition = 0; // 当前位位置

            int bit;
            long decodedIntegers = 0;

            while ((bit = bis.readBit()) != -1) {
                bitPosition = bis.getBitPosition() - 1; // 获取当前位的位置

                if (bit == 0) {
                    currentNode = currentNode.left;
                } else {
                    currentNode = currentNode.right;
                }

                // 如果到达叶子节点，表示一个整数解码完成
                if (currentNode.left == null && currentNode.right == null) {
                    int value = currentNode.value;

                    if (value >= targetValue) {
                        if (value < closestValue) {
                            closestValue = value;
                            positions.clear();
                            positions.add(startPos + bitPosition / 8);
                        } else if (value == closestValue) {
                            positions.add(startPos + bitPosition / 8);
                        }
                    }
                    currentNode = root;
                    decodedIntegers++;

                    // 调试信息：已解码的整数数量
                    if (decodedIntegers % 10_000_000 == 0) {
                        System.out.println("已解码整数数量：" + decodedIntegers);
                    }
                }
            }

            long endTime = System.currentTimeMillis(); // 结束计时

            // 输出结果
            if (closestValue != Integer.MAX_VALUE) {
                System.out.println("查找完成，耗时：" + (endTime - startTime) + " 毫秒。");
                System.out.println("找到的整数值：" + closestValue);
                System.out.println("对应的指针位置（近似）：");
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
            long startPos = header[4];

            int valueToDelete = ClientData.getClosestValue();

            if (valueToDelete == Integer.MAX_VALUE) {
                System.out.println("没有需要删除的整数。");
                return;
            }

            // 删除前的压缩数据长度
            long oldCompressedLength = raf.length() - startPos;
            System.out.println("删除前的压缩数据长度：" + oldCompressedLength);

            long startTime = System.currentTimeMillis(); // 开始计时

            // 解码、删除、重新编码并写回文件
            deleteAndRecompress(raf, startPos, valueToDelete);

            long endTime = System.currentTimeMillis(); // 结束计时

            // 删除后的压缩数据长度
            long newCompressedLength = raf.length() - startPos;
            System.out.println("删除后的压缩数据长度：" + newCompressedLength);

            // 更新文件头信息
            header[5] = newCompressedLength;
            writeHeader(raf, header);

            // 打印新的文件头信息
            System.out.println("更新后的文件头信息：");
            System.out.println("Part A 起始位置：" + header[0] + "，长度：" + header[1]);
            System.out.println("Part B 起始位置：" + header[2] + "，长度：" + header[3]);
            System.out.println("Part C 起始位置：" + header[4] + "，长度：" + header[5]);

            System.out.println("删除和更新操作完成，耗时：" + (endTime - startTime) + " 毫秒。");

            // 验证删除操作是否生效
            verifyDeletion(raf, startPos, valueToDelete);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 删除指定的整数并重新编码
    private static void deleteAndRecompress(RandomAccessFile raf, long startPos, int valueToDelete) throws IOException {
        // 加载霍夫曼树
        HuffmanNode root = loadHuffmanTree();
        if (root == null) {
            System.out.println("无法加载霍夫曼树，无法解压缩数据。");
            return;
        }

        // 解码压缩数据并统计频率
        raf.seek(startPos);
        byte[] compressedData = new byte[(int) (raf.length() - startPos)];
        raf.readFully(compressedData);

        System.out.println("客户端3开始第一遍解码，统计频率...");

        BitInputStream bis = new BitInputStream(compressedData);
        int[] frequencies = new int[MAX_VALUE + 1];

        HuffmanNode currentNode = root;
        int bit;
        long decodedIntegers = 0;
        long bitPosition = 0;

        List<Long> deletePositions = new ArrayList<>(); // 用于记录被删除的整数位置

        while ((bit = bis.readBit()) != -1) {
            bitPosition = bis.getBitPosition() - 1;

            if (bit == 0) {
                currentNode = currentNode.left;
            } else {
                currentNode = currentNode.right;
            }

            if (currentNode.left == null && currentNode.right == null) {
                int value = currentNode.value;

                if (value != valueToDelete) {
                    frequencies[value]++;
                } else {
                    // 记录被删除的整数位置
                    deletePositions.add(startPos + bitPosition / 8);
                }
                currentNode = root;
                decodedIntegers++;
            }
        }

        System.out.println("第一遍解码完成，共解码整数数量：" + decodedIntegers);
        System.out.println("被删除的整数值：" + valueToDelete);
        System.out.println("对应的指针位置（近似）：");
        for (long pos : deletePositions) {
            System.out.println("    位置：" + pos);
        }

        // 重新构建霍夫曼树和编码
        HuffmanNode newRoot = buildHuffmanTree(frequencies);
        Map<Integer, HuffmanCode> huffmanCodes = new HashMap<>();
        generateCodes(newRoot, 0L, 0, huffmanCodes);

        // 保存新的霍夫曼树
        saveHuffmanTree(newRoot);

        // 准备进行第二遍解码和重新编码
        System.out.println("客户端3开始第二遍解码并重新编码数据...");

        // 重新定位到数据开始位置
        bis = new BitInputStream(compressedData);
        currentNode = root;

        // 准备写入新数据
        raf.setLength(startPos); // 截断文件，准备写入新的压缩数据
        raf.seek(startPos);

        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(raf.getFD()));

        long bitBuffer = 0L;
        int bitBufferLength = 0;

        decodedIntegers = 0;

        while ((bit = bis.readBit()) != -1) {
            if (bit == 0) {
                currentNode = currentNode.left;
            } else {
                currentNode = currentNode.right;
            }

            if (currentNode.left == null && currentNode.right == null) {
                int value = currentNode.value;

                if (value != valueToDelete) {
                    // 获取新的编码
                    HuffmanCode code = huffmanCodes.get(value);

                    bitBuffer = (bitBuffer << code.codeLength) | code.codeBits;
                    bitBufferLength += code.codeLength;

                    while (bitBufferLength >= 8) {
                        bitBufferLength -= 8;
                        int byteToWrite = (int) ((bitBuffer >> bitBufferLength) & 0xFF);
                        bos.write(byteToWrite);
                    }
                } else {
                    // 打印被删除的整数值和位置
                    long deletePos = startPos + bis.getBytePosition() - 1;
                    System.out.println("删除整数：" + value + "，位置：" + deletePos);
                }

                currentNode = newRoot;
                decodedIntegers++;
            }
        }

        // 处理剩余的位
        if (bitBufferLength > 0) {
            int byteToWrite = (int) ((bitBuffer << (8 - bitBufferLength)) & 0xFF);
            bos.write(byteToWrite);
        }

        bos.flush();
        bos.close(); // 确保缓冲区的数据全部写入

        // 获取当前文件指针位置
        long newFilePointer = raf.getFilePointer();

        // 截断文件到当前写入位置
        raf.setLength(newFilePointer);

        System.out.println("客户端3重新编码完成，文件已更新。");
    }

    // 验证删除操作是否生效
    private static void verifyDeletion(RandomAccessFile raf, long startPos, int valueToDelete) throws IOException {
        // 加载新的霍夫曼树
        HuffmanNode root = loadHuffmanTree();
        if (root == null) {
            System.out.println("无法加载霍夫曼树，无法验证删除操作。");
            return;
        }

        // 读取新的压缩数据
        raf.seek(startPos);
        byte[] compressedData = new byte[(int) (raf.length() - startPos)];
        raf.readFully(compressedData);

        BitInputStream bis = new BitInputStream(compressedData);
        HuffmanNode currentNode = root;

        int bit;
        boolean valueFound = false;

        while ((bit = bis.readBit()) != -1) {
            if (bit == 0) {
                currentNode = currentNode.left;
            } else {
                currentNode = currentNode.right;
            }

            if (currentNode.left == null && currentNode.right == null) {
                int value = currentNode.value;

                if (value == valueToDelete) {
                    valueFound = true;
                    break;
                }
                currentNode = root;
            }
        }

        if (valueFound) {
            System.out.println("验证结果：被删除的整数 " + valueToDelete + " 仍然存在！");
        } else {
            System.out.println("验证结果：被删除的整数 " + valueToDelete + " 已被成功删除。");
        }
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

    // 加载霍夫曼树
    private static HuffmanNode loadHuffmanTree() {
        try {
            ObjectInputStream ois = new ObjectInputStream(new FileInputStream(HUFFMAN_TREE_FILE));
            HuffmanNode root = (HuffmanNode) ois.readObject();
            ois.close();
            return root;
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    // 保存霍夫曼树
    private static void saveHuffmanTree(HuffmanNode root) {
        try {
            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(HUFFMAN_TREE_FILE));
            oos.writeObject(root);
            oos.close();
            System.out.println("客户端3已保存新的霍夫曼树。");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 构建霍夫曼树
    private static HuffmanNode buildHuffmanTree(int[] frequencies) {
        PriorityQueue<HuffmanNode> pq = new PriorityQueue<>();
        for (int i = 1; i <= MAX_VALUE; i++) {
            if (frequencies[i] > 0) {
                pq.add(new HuffmanNode(i, frequencies[i]));
            }
        }

        while (pq.size() > 1) {
            HuffmanNode left = pq.poll();
            HuffmanNode right = pq.poll();
            HuffmanNode parent = new HuffmanNode(-1, left.frequency + right.frequency);
            parent.left = left;
            parent.right = right;
            pq.add(parent);
        }
        return pq.poll();
    }

    // 生成霍夫曼编码
    private static void generateCodes(HuffmanNode node, long codeBits, int codeLength, Map<Integer, HuffmanCode> huffmanCodes) {
        if (node != null) {
            if (node.value != -1) {
                huffmanCodes.put(node.value, new HuffmanCode(codeBits, codeLength));
            } else {
                generateCodes(node.left, (codeBits << 1), codeLength + 1, huffmanCodes);
                generateCodes(node.right, (codeBits << 1) | 1, codeLength + 1, huffmanCodes);
            }
        }
    }

    // 辅助类
    static class HuffmanCode {
        long codeBits;
        int codeLength;

        HuffmanCode(long codeBits, int codeLength) {
            this.codeBits = codeBits;
            this.codeLength = codeLength;
        }
    }

    static class BitInputStream {
        private byte[] data;
        private int bytePos;
        private int bitPos;

        public BitInputStream(byte[] data) {
            this.data = data;
            this.bytePos = 0;
            this.bitPos = 0;
        }

        public int readBit() {
            if (bytePos >= data.length) {
                return -1; // 已到达数据末尾
            }
            int bit = (data[bytePos] >> (7 - bitPos)) & 1;
            bitPos++;
            if (bitPos == 8) {
                bitPos = 0;
                bytePos++;
            }
            return bit;
        }

        public long getBitPosition() {
            return (long) bytePos * 8 + bitPos;
        }

        public int getBytePosition() {
            return bytePos;
        }
    }

    // 辅助类，用于保存客户端的数据
    static class ClientData {
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
}

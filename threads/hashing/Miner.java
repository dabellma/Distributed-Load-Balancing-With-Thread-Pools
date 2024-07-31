package csx55.threads.hashing;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

public class Miner {
    private static final Miner instance = new Miner();
    private static final int LEADING_ZEROS = 17;

    public static Miner getInstance() {
        return instance;
    }

    private Miner() {
    }

    private int leadingZeros(byte[] hash) {
        int count = 0;
        for (byte b : hash) {
            if (b == 0) {
                count += 8;
            } else {
                int i = 0x80;
                while ((b & i) == 0) {
                    count++;
                    i >>= 1;
                }
                break;
            }
        }
        return count;
    }

    public void mine(Task task) throws NoSuchAlgorithmException {
        task.setThreadId();
        Random random = new Random();
        byte[] hash;
        MessageDigest sha256 = MessageDigest.getInstance("SHA3-256");
        while (true) {
            task.setTimestamp();
            task.setNonce(random.nextInt());
            hash = sha256.digest(task.toBytes());
            if (leadingZeros(hash) >= LEADING_ZEROS) {
                break;
            }
        }
    }

    public static void main(String[] args) throws NoSuchAlgorithmException {
        // Mining
        Miner miner = Miner.getInstance();
        Task task = new Task("192.168.0.1", 1234, 1, new Random().nextInt());
        miner.mine(task);

        // Verification
        MessageDigest sha256 = MessageDigest.getInstance("SHA3-256");
        int leadingZeros = miner.leadingZeros(sha256.digest(task.toBytes()));
        System.out.println("Task: " + task + " Leading zeros: " + leadingZeros);
    }
}
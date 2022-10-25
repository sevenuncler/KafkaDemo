public class App {
    public static void main(String[] args) {
        System.out.print("hello world\n");

        ProducerThread producerThread = new ProducerThread();
        producerThread.start();

        ConsumerThread consumerThread = new ConsumerThread();
        consumerThread.start();

        ConsumerThread consumerThread1 = new ConsumerThread();
        consumerThread1.start();

        // 发送
        while (true) {
        }
    }
}

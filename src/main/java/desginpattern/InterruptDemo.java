package desginpattern;

public class InterruptDemo {
//    private static int i;

    //    public static void main(String[] args) throws InterruptedException {
//        Thread thread = new Thread(() -> {
//            while (true) {
//                if (Thread.currentThread().isInterrupted()) {
//                    System.out.println("before:" + Thread.currentThread().isInterrupted());
//                    Thread.interrupted(); //对线程进行复位，由true变成false
//                    System.out.println("after:" + Thread.currentThread().isInterrupted());
//                }
//            }
//        }, "interruptDemo");
//        thread.start();
//        TimeUnit.SECONDS.sleep(1);
//        thread.interrupt();
    private static int count = 0;

    public static void inc() {
        synchronized (InterruptDemo.class) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            count++;
        }

    }

    public static void main(String[] args) throws InterruptedException {
        for (int i = 0; i < 1000; i++) {
            new Thread(() -> InterruptDemo.inc()).start();
        }
        Thread.sleep(3000);
        System.out.println("运行结果" + count);
    }
}


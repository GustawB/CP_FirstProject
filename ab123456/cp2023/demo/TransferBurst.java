/*
 * University of Warsaw
 * Concurrent Programming Course 2023/2024
 * Java Assignment
 *
 * Author: Konrad Iwanicki (iwanicki@mimuw.edu.pl)
 */
package cp2023.demo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import cp2023.base.ComponentId;
import cp2023.base.ComponentTransfer;
import cp2023.base.DeviceId;
import cp2023.base.StorageSystem;
import cp2023.exceptions.TransferException;
import cp2023.solution.StorageSystemFactory;


public final class TransferBurst {
    private static int testNr = 1;
    private static void message(){
        System.out.println("");
        System.out.println("Quick note: those tests are based on the tests made by Iwanicki, and so they are fundamentally flawed,");
        System.out.println("eg. moodleCycles. Tests like it require that processes will be accessing the system in a very");
        System.out.println("specific manner, and it's difficult to do. It should work with equal durations and sleep times");
        System.out.println("that I used, by it may not always work as intended. So use at your own risk.");
        System.out.println("");
    }
    public static void main(String[] args) {
        message();

        StorageSystem system = setupSystem();
        runTest(threeElementsCycle(system));//It's here only because I had an error.

        system = setupSystem();
        runTest(independentCycles(system));

        system = setupSystem();
        runTest(moodleCycles(system));

        system = setupSystem();
        runTest(longSequence(system));

        system = setupSystem();
        runTest(longSequencePermutation(system));

        system = setupSystem();
        runTest(bulkDeleteAndAdd(system));

        //Variation on the bulkDeleteAndAdd, but here I'm deleting 101 and adding 101,
        //not deleting 101 and adding 111. Czytelnik koneser zrozumie.
        system = setupSystem();
        runTest(bulkDelete(system));
        sleep(1000);
        runTest(bulkAdd(system));
        //This time I won't set up the new system so I can test how cycles work.
        sleep(1000);
        runTest(moodleCycles(system));

        System.out.println("");
        System.out.println("Congrats, your code finished execution (and it maybe even works (\\s))");
        testNr = 1;
    }

    private static void printTestNr(){
        System.out.println("");
        System.out.println("Test " + testNr + " passed");
        System.out.println("");
        ++testNr;
    }

    private static void runTest(Collection<Thread> test){
        runTransferers(test);
        printTestNr();
    }

    private final static StorageSystem setupSystem() {
        DeviceId dev1 = new DeviceId(1);
        DeviceId dev2 = new DeviceId(2);
        DeviceId dev3 = new DeviceId(3);
        DeviceId dev4 = new DeviceId(4);
        DeviceId dev5 = new DeviceId(5);
        DeviceId dev6 = new DeviceId(6);

        ComponentId comp1 = new ComponentId(101);
        ComponentId comp2 = new ComponentId(102);
        ComponentId comp3 = new ComponentId(103);
        ComponentId comp4 = new ComponentId(104);
        ComponentId comp5 = new ComponentId(105);
        ComponentId comp6 = new ComponentId(106);
        ComponentId comp7 = new ComponentId(107);
        ComponentId comp8 = new ComponentId(108);
        ComponentId comp9 = new ComponentId(109);
        ComponentId comp11 = new ComponentId(111);
        ComponentId comp12 = new ComponentId(112);
        ComponentId comp13 = new ComponentId(113);
        ComponentId comp14 = new ComponentId(114);
        ComponentId comp15 = new ComponentId(115);
        ComponentId comp16 = new ComponentId(116);
        ComponentId comp17 = new ComponentId(117);
        ComponentId comp18 = new ComponentId(118);
        ComponentId comp19 = new ComponentId(119);
        
        HashMap<DeviceId, Integer> deviceCapacities = new HashMap<>(6);
        deviceCapacities.put(dev1, 3);
        deviceCapacities.put(dev2, 3);
        deviceCapacities.put(dev3, 3);
        deviceCapacities.put(dev4, 3);
        deviceCapacities.put(dev5, 3);
        deviceCapacities.put(dev6, 3);

        HashMap<ComponentId, DeviceId> initialComponentMapping = new HashMap<>(18);

        initialComponentMapping.put(comp1, dev1);
        initialComponentMapping.put(comp2, dev1);
        initialComponentMapping.put(comp3, dev1);
        
        initialComponentMapping.put(comp4, dev2);
        initialComponentMapping.put(comp5, dev2);
        initialComponentMapping.put(comp6, dev2);
        
        initialComponentMapping.put(comp7, dev3);
        initialComponentMapping.put(comp8, dev3);
        initialComponentMapping.put(comp9, dev3);

        initialComponentMapping.put(comp11, dev4);
        initialComponentMapping.put(comp12, dev4);
        initialComponentMapping.put(comp13, dev4);

        initialComponentMapping.put(comp14, dev5);
        initialComponentMapping.put(comp15, dev5);
        initialComponentMapping.put(comp16, dev5);

        initialComponentMapping.put(comp17, dev6);
        initialComponentMapping.put(comp18, dev6);
        initialComponentMapping.put(comp19, dev6);
        
        return StorageSystemFactory.newSystem(deviceCapacities, initialComponentMapping);
    }

    private static Thread oneActivityPerThread(StorageSystem system, int compId, int srcDevId, int dstDevId, int duration, int sleep){
        return new Thread(new Runnable() {
            @Override
            public void run() {
                if(sleep > 0){
                    sleep(sleep);
                }
                System.out.println("Transferer " + Thread.currentThread().getId() + " has started.");
                executeTransfer(system, compId, srcDevId, dstDevId, duration);
                System.out.println("Transferer " + Thread.currentThread().getId() + " has finished.");
            }
        });
    }

    private final static Collection<Thread> threeElementsCycle(StorageSystem system) {
        ArrayList<Thread> transferer = new ArrayList<>();
        transferer.add(oneActivityPerThread(system, 101, 1, 2, 10, 0));
        transferer.add(oneActivityPerThread(system, 107, 3, 1, 10, 0));
        transferer.add(oneActivityPerThread(system, 104, 2, 3, 10, 0));
        return transferer;
    }
    
    private final static Collection<Thread> independentCycles(StorageSystem system) {
        ArrayList<Thread> transferer = new ArrayList<>();
        transferer.add(oneActivityPerThread(system, 101, 1, 2, 10, 0));
        transferer.add(oneActivityPerThread(system, 111, 4, 5, 10, 0));
        transferer.add(oneActivityPerThread(system, 107, 3, 1, 10, 0));
        transferer.add(oneActivityPerThread(system, 117, 6, 4, 10, 0));
        transferer.add(oneActivityPerThread(system, 104, 2, 3, 10, 0));
        transferer.add(oneActivityPerThread(system, 114, 5, 6, 10, 0));
        return transferer;
    }

    private final static Collection<Thread> moodleCycles(StorageSystem system) {
        ArrayList<Thread> transferer = new ArrayList<>();
        transferer.add(oneActivityPerThread(system, 101, 1, 2, 10, 0));
        transferer.add(oneActivityPerThread(system, 111, 4, 2, 10, 10));
        transferer.add(oneActivityPerThread(system, 107, 3, 4, 10, 20));
        transferer.add(oneActivityPerThread(system, 108, 3, 1, 10, 30));
        transferer.add(oneActivityPerThread(system, 104, 2, 3, 10, 40));
        transferer.add(oneActivityPerThread(system, 106, 2, 0, 10, 50));
        return transferer;
    }

    private final static Collection<Thread> longSequence(StorageSystem system) {
        ArrayList<Thread> transferer = new ArrayList<>();
        transferer.add(oneActivityPerThread(system, 169, 0, 1, 10, 0));
        transferer.add(oneActivityPerThread(system, 101, 1, 2, 10, 10));
        transferer.add(oneActivityPerThread(system, 104, 2, 3, 10, 20));
        transferer.add(oneActivityPerThread(system, 107, 3, 4, 10, 30));
        transferer.add(oneActivityPerThread(system, 111, 4, 5, 10, 40));
        transferer.add(oneActivityPerThread(system, 114, 5, 6, 10, 50));
        transferer.add(oneActivityPerThread(system, 117, 6, 0, 10, 60));
        return transferer;
    }

    private final static Collection<Thread> longSequencePermutation(StorageSystem system) {
        ArrayList<Thread> transferer = new ArrayList<>();
        transferer.add(oneActivityPerThread(system, 169, 0, 1, 10, 0));
        transferer.add(oneActivityPerThread(system, 114, 5, 6, 10, 50));
        transferer.add(oneActivityPerThread(system, 104, 2, 3, 10, 20));
        transferer.add(oneActivityPerThread(system, 111, 4, 5, 10, 40));
        transferer.add(oneActivityPerThread(system, 117, 6, 0, 10, 60));
        transferer.add(oneActivityPerThread(system, 107, 3, 4, 10, 30));
        transferer.add(oneActivityPerThread(system, 101, 1, 2, 10, 10));
        return transferer;
    }

    private final static Collection<Thread> bulkDeleteAndAdd(StorageSystem system) {
        ArrayList<Thread> transferer = new ArrayList<>();
        //Deletion.
        for(int i = 0; i < 9; ++i){
            transferer.add(oneActivityPerThread(system, 100 + i + 1, i/3 +1, 0, 10, 0));
        }
        //Addition
        for(int i = 0; i < 9; ++i){
            transferer.add(oneActivityPerThread(system, 110 + i + 1, 0, i/3 +1, 10, 0));
        }
        return transferer;
    }

    private final static Collection<Thread> bulkDelete(StorageSystem system) {
        ArrayList<Thread> transferer = new ArrayList<>();
        //Deletion.
        for(int i = 0; i < 9; ++i){
            transferer.add(oneActivityPerThread(system, 100 + i + 1, i/3 +1, 0, 10, 0));
        }
        return transferer;
    }

    private final static Collection<Thread> bulkAdd(StorageSystem system) {
        ArrayList<Thread> transferer = new ArrayList<>();
        //Addition
        for(int i = 0; i < 9; ++i){
            transferer.add(oneActivityPerThread(system, 100 + i + 1, 0, i/3 +1, 10, 0));
        }
        return transferer;
    }
    
    private final static void runTransferers(Collection<Thread> users) {
        for (Thread t : users) {
            t.start();
        }
        for (Thread t : users) {
            try {
                t.join();
            } catch (InterruptedException e) {
                throw new RuntimeException("panic: unexpected thread interruption", e);
            }
        }
    }

    
    
    private final static CompTransfImpl executeTransfer(
            StorageSystem system,
            int compId,
            int srcDevId,
            int dstDevId,
            long duration
    ) {
        CompTransfImpl transfer =
                new CompTransfImpl(
                        new ComponentId(compId),
                        srcDevId > 0 ? new DeviceId(srcDevId) : null,
                        dstDevId > 0 ? new DeviceId(dstDevId) : null,
                        duration
                );
        try {
            //System.out.println("Try to execute transfer from " + srcDevId +" to " + dstDevId);
            system.execute(transfer);
            //System.out.println("Managed to get out of the " + srcDevId +" to " + dstDevId);
        } catch (TransferException e) {
            throw new RuntimeException("Uexpected transfer exception: " + e.toString(), e);
        }
        return transfer;
    }
    
    private final static void sleep(long duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            throw new RuntimeException("panic: unexpected thread interruption", e);
        }
    }
    

    
    private final static class CompTransfImpl implements ComponentTransfer {
        private static int uidGenerator = 0;
        private final int uid;
        private final long owningThread;
        private final Integer phantomSynchronizer;
        private final ComponentId compId;
        private final DeviceId srcDevId;
        private final DeviceId dstDevId;
        private final long duration;
        private boolean prepared;
        private boolean started;
        private boolean done;
        
        private final static synchronized int generateUID() {
            return ++uidGenerator;
        }
        
        public CompTransfImpl(
                ComponentId compId,
                DeviceId srcDevId,
                DeviceId dstDevId,
                long duration
        ) {
            this.uid = generateUID();
            this.phantomSynchronizer = 19;
            this.owningThread = Thread.currentThread().getId();
            this.compId = compId;
            this.srcDevId = srcDevId;
            this.dstDevId = dstDevId;
            this.duration = duration;
            this.prepared = false;
            this.started = false;
            this.done = false;
            System.out.println("Transferer " + this.owningThread +
                    " is about to issue transfer " + this.uid +
                    " of " + this.compId + " from " + this.srcDevId +
                    " to " + this.dstDevId + ".");
        }
        
        @Override
        public ComponentId getComponentId() {
            return this.compId;
        }

        @Override
        public DeviceId getSourceDeviceId() {
            return this.srcDevId;
        }

        @Override
        public DeviceId getDestinationDeviceId() {
            return this.dstDevId;
        }

        @Override
        public void prepare() {
            synchronized (this.phantomSynchronizer) {
                if (this.prepared) {
                    throw new RuntimeException(
                            "Transfer " + this.uid + " is being prepared more than once!");
                }
                if (this.owningThread != Thread.currentThread().getId()) {
                    throw new RuntimeException(
                            "Transfer " + this.uid +
                            " is being prepared by a different thread that scheduled it!");
                }
                this.prepared = true;
            }
            System.out.println("Transfer " + this.uid + " of " + this.compId +
                    " from " + this.srcDevId + " to " + this.dstDevId +
                    " has been prepared by user " + Thread.currentThread().getId() + ".");
        }

        @Override
        public void perform() {
            synchronized (this.phantomSynchronizer) {
                if (! this.prepared) {
                    throw new RuntimeException(
                            "Transfer " + this.uid + " has not been prepared " +
                            "before being performed!");
                }
                if (this.started) {
                    throw new RuntimeException(
                            "Transfer " + this.uid + " is being started more than once!");
                }
                if (this.owningThread != Thread.currentThread().getId()) {
                    throw new RuntimeException(
                            "Transfer " + this.uid +
                            " is being performed by a different thread that scheduled it!");
                }
                this.started = true;
            }
            System.out.println("Transfer " + this.uid + " of " + this.compId +
                    " from " + this.srcDevId + " to " + this.dstDevId + " has been started.");
            sleep(this.duration);
            synchronized (this.phantomSynchronizer) {
                this.done = true;
            }
            System.out.println("Transfer " + this.uid + " of " + this.compId +
                    " from " + this.srcDevId + " to " + this.dstDevId + " has been completed.");
        }
        
    }
    
}

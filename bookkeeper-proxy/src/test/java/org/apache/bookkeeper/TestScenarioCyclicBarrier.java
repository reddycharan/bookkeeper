package org.apache.bookkeeper;

import java.util.List;
import java.util.concurrent.CyclicBarrier;

public class TestScenarioCyclicBarrier extends CyclicBarrier {

    public TestScenarioCyclicBarrier(int parties, TestScenarioState testScenarioState) {
        super(parties, new TestScenarioBarrierAction(testScenarioState));
    }

    private static class TestScenarioBarrierAction implements Runnable {
        TestScenarioState testScenarioState;

        private TestScenarioBarrierAction(TestScenarioState testScenarioState) {
            this.testScenarioState = testScenarioState;
        }

        @Override
        public void run() {
            int currentTimeSlot = testScenarioState.getCurrentTimeSlot();
            boolean isFailed = false;
            if (currentTimeSlot != -1) {
                List<BKPOperation> currentTimeSlotOperations = testScenarioState.getBKPOperations(currentTimeSlot);
                if (currentTimeSlotOperations != null) {
                    for (BKPOperation currentTimeSlotOperation : currentTimeSlotOperations) {
                        if (currentTimeSlotOperation.isOperationFailed()) {
                            isFailed = true;
                            testScenarioState.setScenarioFailed(true);
                            testScenarioState.addFailedoperation(currentTimeSlotOperation);
                        }
                    }
                }
            }
            for (String threadId : testScenarioState.getThreadIds()) {
                BKPClientThread clientThread = testScenarioState.getBKPClientThread(threadId);
                clientThread.setNextOperation(null);
            }
            if (!isFailed) {
                int nextTimeSlot = currentTimeSlot + 1;
                int numberOfTimeSlots = testScenarioState.getNumberOfTimeSlots();
                if (nextTimeSlot == (numberOfTimeSlots)) {
                    testScenarioState.setScenarioDone(true);
                } else {
                    List<BKPOperation> nextTimeSlotOperations = testScenarioState.getBKPOperations(nextTimeSlot);
                    if (nextTimeSlotOperations != null) {
                        for (BKPOperation bkpOperation : nextTimeSlotOperations) {
                            String threadId = bkpOperation.getThreadId();
                            BKPClientThread clientThread = testScenarioState.getBKPClientThread(threadId);
                            clientThread.setNextOperation(bkpOperation);
                        }
                    }
                    testScenarioState.setCurrentTimeSlot(nextTimeSlot);
                }
            }
        }
    }
}

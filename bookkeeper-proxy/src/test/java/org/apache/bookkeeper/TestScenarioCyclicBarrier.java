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
                List<Operation> currentTimeSlotOperations = testScenarioState.getOperations(currentTimeSlot);
                if (currentTimeSlotOperations != null) {
                    for (Operation currentTimeSlotOperation : currentTimeSlotOperations) {
                        if (currentTimeSlotOperation.isOperationFailed()) {
                            isFailed = true;
                            testScenarioState.setScenarioFailed(true);
                            testScenarioState.addFailedOperation(currentTimeSlotOperation);
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
                    List<Operation> nextTimeSlotOperations = testScenarioState.getOperations(nextTimeSlot);
                    if (nextTimeSlotOperations != null) {
                        testScenarioState.setAdditionalTimeoutWaitTime(0);
                        for (Operation operation : nextTimeSlotOperations) {
                            String threadId = operation.getThreadId();
                            BKPClientThread clientThread = testScenarioState.getBKPClientThread(threadId);
                            clientThread.setNextOperation(operation);
                            operation.preSetup(testScenarioState);
                        }
                    }
                    testScenarioState.setCurrentTimeSlot(nextTimeSlot);
                }
            }
        }
    }
}

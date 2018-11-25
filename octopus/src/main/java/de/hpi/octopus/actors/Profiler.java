package de.hpi.octopus.actors;

import java.io.Serializable;
import java.util.*;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import de.hpi.octopus.actors.Worker.WorkMessage;
import lombok.AllArgsConstructor;
import lombok.Data;

public class Profiler extends AbstractActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "profiler";

	public static Props props() {
		return Props.create(Profiler.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////
	
	@Data @AllArgsConstructor
	public static class RegistrationMessage implements Serializable {
		private static final long serialVersionUID = 4545299661052078209L;
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class TaskMessage implements Serializable {
		private static final long serialVersionUID = -8330958742629706628L;
		private TaskMessage() {}
		private HashMap<Integer, String> hashedPasswords;
		private HashMap<Integer, String> genes;
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class TaskMessageCrackPasswords implements Serializable {
		private static final long serialVersionUID = -8330958742629706627L;
		private TaskMessageCrackPasswords() {}
		private HashMap<Integer, String> hashedPasswords;
	}
	
	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public abstract static class CompletionMessage implements Serializable {
		private static final long serialVersionUID = -6823011111281387872L;
		public enum status {SUCCESS, FAILED}
		private CompletionMessage() {}
		protected status result;
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class CompletionMessagePasswordCracking extends Profiler.CompletionMessage {
		private static final long serialVersionUID = -6823000111281387872L;
		private CompletionMessagePasswordCracking() {}
		protected status result;
		private String[] hashes;
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class CompletionMessageLinearCombination extends Profiler.CompletionMessage {
		private static final long serialVersionUID = -6823011111281007872L;
		private CompletionMessageLinearCombination() {}
		protected status result;
		private boolean found;
		private int[] prefixes;
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class CompletionMessageGeneComparsion extends Profiler.CompletionMessage {
		private static final long serialVersionUID = -6823000111281007872L;
		private CompletionMessageGeneComparsion() {}
		protected status result;
		private int length;
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class CompletionMessageFindHash extends Profiler.CompletionMessage {
		private static final long serialVersionUID = -6823000111281007815L;
		private CompletionMessageFindHash() {}
		protected status result;
		private String hash;
	}

	/////////////////
	// Actor State //
	/////////////////
	
	private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

	private final Queue<WorkMessage> unassignedWork = new LinkedList<>();
	private final Queue<ActorRef> idleWorkers = new LinkedList<>();
	private final Map<ActorRef, WorkMessage> busyWorkers = new HashMap<>();

	private final Map<String, String> calculatedPasswordHashes = new HashMap<>();
	private List<Integer> crackedPasswordsAsInteger = new ArrayList<>();
	private List<List<Integer>> currentRow = new ArrayList<>();
	private int currentRowNumber;
	private final Map<Integer, String> plainTextPasswords = new HashMap<>();
	private final Map<Integer, Integer> prefixes = new HashMap<>();
	private final Map<Integer, Pair> longestGeneSubString = new HashMap<>();
	private Map<Integer, String> genes = new HashMap<>();
	private final Map<Integer, String> finalHashes = new HashMap<>();

	@Data @AllArgsConstructor
	private final class Pair {
		public int partnerId;
		public int length;
	}


	private TaskMessageCrackPasswords task;

	////////////////////
	// Actor Behavior //
	////////////////////
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(RegistrationMessage.class, this::handle)
				.match(Terminated.class, this::handle)
				.match(TaskMessage.class, this::handle)
				.match(TaskMessageCrackPasswords.class, this::handle)
				.match(CompletionMessage.class, this::handle)
				.matchAny(object -> this.log.info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(RegistrationMessage message) {
		this.context().watch(this.sender());
		
		this.assign(this.sender());
		this.log.info("Registered {}", this.sender());
	}
	
	private void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		
		if (!this.idleWorkers.remove(message.getActor())) {
			WorkMessage work = this.busyWorkers.remove(message.getActor());
			if (work != null) {
				this.assign(work);
			}
		}		
		this.log.info("Unregistered {}", message.getActor());
	}

	private void handle(TaskMessage message){
		this.handle(new TaskMessageCrackPasswords(message.hashedPasswords));
		this.genes = message.genes;
	}
	
	private void handle(TaskMessageCrackPasswords message) {
		if (this.task != null)
			this.log.error("The profiler actor can process only one task in its current implementation!");
		
		this.task = message;


		String[] sixDigitNumbers = this.calculateSixDigitNumbers();

		for (int i = 0; i<sixDigitNumbers.length; i += 100){
			this.assign(new Worker.WorkMessagePasswordCracking(Arrays.copyOfRange(sixDigitNumbers, i, i+100)));
		}

	}
	
	private void handle(CompletionMessage message) {
		ActorRef worker = this.sender();
		WorkMessage work = this.busyWorkers.remove(worker);

		//this.log.info("Completed: [{},{}]", Arrays.toString(work.getX()), Arrays.toString(work.getY()));
		
		switch (message.getResult()) {
			case SUCCESS:
				if(work instanceof Worker.WorkMessagePasswordCracking){
					CompletionMessagePasswordCracking completionMessage = (CompletionMessagePasswordCracking) message;
					Worker.WorkMessagePasswordCracking crackingWork = (Worker.WorkMessagePasswordCracking) work;
					this.report(completionMessage);
					for (int i = 0; i < crackingWork.getSixDigitNumbers().length; i++){
						this.calculatedPasswordHashes.put(completionMessage.hashes[i], crackingWork.getSixDigitNumbers()[i]);
					}
					if (unassignedWork.isEmpty() && busyWorkers.isEmpty()){
						this.crackPasswords();
						this.calculateLinearCombination();
					}
				}
				else if (work instanceof Worker.WorkMessageLinearCombination){
					CompletionMessageLinearCombination completionMessage = (CompletionMessageLinearCombination) message;
					Worker.WorkMessageLinearCombination workMessage = (Worker.WorkMessageLinearCombination) work;
					this.report(completionMessage);

                    if (!completionMessage.found) {
                        this.calculateLinearCombination();
                    } else {
						this.unassignedWork.clear();

                        int[] prefixes = completionMessage.prefixes;

                        int i = 0;
                        for (Integer id : this.plainTextPasswords.keySet()){
                            this.prefixes.put(id, prefixes[i]);
                            i++;
                        }
                        this.log.info("FINAL PASSWORDS WITH PREFIX 1: " + Arrays.toString(prefixes));
                        this.startGeneComparision();
					}
				}
				else if (work instanceof Worker.WorkMessageGeneComparision){
					CompletionMessageGeneComparsion completionMessage = (CompletionMessageGeneComparsion) message;
					Worker.WorkMessageGeneComparision workMessage = (Worker.WorkMessageGeneComparision) work;
					this.report(completionMessage);

					final int currentLength1 = longestGeneSubString.getOrDefault(workMessage.getFirstID(), new Pair(-1, 0)).length;
					final int currentLength2 = longestGeneSubString.getOrDefault(workMessage.getSecondID(), new Pair(-1, 0)).length;

					if (completionMessage.length > currentLength1) {
						longestGeneSubString.put(workMessage.getFirstID(), new Pair(workMessage.getSecondID(), completionMessage.length));
					}
					if (completionMessage.length > currentLength2) {
						longestGeneSubString.put(workMessage.getSecondID(), new Pair(workMessage.getFirstID(), completionMessage.length));
					}

					if (unassignedWork.isEmpty() && busyWorkers.isEmpty()){
						this.log.info("Calculated longest gene overlaps" + Arrays.toString(longestGeneSubString.values().toArray()));

						this.prefixes.forEach((id, prefix) -> this.assign(new Worker.WorkMessageFindHash(id, prefix == 1 ? "1" : "0", longestGeneSubString.get(id).partnerId)));
					}
				} else if (work instanceof Worker.WorkMessageFindHash) {
					CompletionMessageFindHash completionMessage = (CompletionMessageFindHash) message;
					Worker.WorkMessageFindHash workMessage = (Worker.WorkMessageFindHash) work;
					this.report(completionMessage);

					finalHashes.put(workMessage.getId(), completionMessage.hash);

					if (unassignedWork.isEmpty() && busyWorkers.isEmpty()){
						this.log.info("Done!!!");

						finalHashes.forEach((id, hash) -> this.log.warning("ID: " + id + " Hash: " + hash));
					}
				}
				break;
			case FAILED:
				this.assign(work);
				break;
		}
		
		this.assign(worker);
	}

	private void assign(WorkMessage work) {
		ActorRef worker = this.idleWorkers.poll();
		
		if (worker == null) {
			this.unassignedWork.add(work);
			return;
		}
		
		this.busyWorkers.put(worker, work);
		worker.tell(work, this.self());
	}
	
	private void assign(ActorRef worker) {
		WorkMessage work = this.unassignedWork.poll();
		
		if (work == null) {
			this.idleWorkers.add(worker);
			return;
		}
		
		this.busyWorkers.put(worker, work);
		worker.tell(work, this.self());
	}
	
	private void report(CompletionMessagePasswordCracking completion) {
		this.log.info("Finished hashes. First Hash in package: " + completion.getHashes()[0]);
	}

	private void report(CompletionMessageLinearCombination completion) {
		this.log.info("Finished part of line.");
	}

	private void report(CompletionMessageGeneComparsion completion) {
		this.log.info("Finished comparing two genes.");
	}

	private void report(CompletionMessageFindHash completion) { this.log.info("Found hash " + completion.hash); }

	private String[] calculateSixDigitNumbers(){
		String[] sixDigitNumbers = new String[1000000];
		for (int i=0; i<=999999; i++){
			sixDigitNumbers[i] = String.format("%06d", i);
		}
		return sixDigitNumbers;
	}

	private void crackPasswords(){
		for (Map.Entry<Integer,String> entry : this.task.hashedPasswords.entrySet()){
			Integer id = entry.getKey();
			String originalHash = entry.getValue();

			String crackedPassword = this.calculatedPasswordHashes.get(originalHash);
			this.plainTextPasswords.put(id, crackedPassword);
			this.crackedPasswordsAsInteger.add(Integer.parseInt(crackedPassword));
		}
		this.log.info("PASSWORDS CRACKED!");

		/*
		for(Map.Entry<Integer,String> entry : result.entrySet()){
			Integer id = entry.getKey();
			String originalPassword = entry.getValue();

			this.log.info("Password " + id + " : " + originalPassword);
		}
		*/

		for (int value : this.crackedPasswordsAsInteger){
			this.log.info(Integer.toString(value) + "; ");
		}
	}

	private long nextToSendLinearCombinationValue = 0;

	private	void calculateLinearCombination(){
	    final int packageSize = 10000;

	    for (int i = 0; i < 50 - unassignedWork.size(); i++) {
            this.assign(new Worker.WorkMessageLinearCombination(
                    this.crackedPasswordsAsInteger.stream().mapToInt(j -> j).toArray(),
                    nextToSendLinearCombinationValue,
                    nextToSendLinearCombinationValue + packageSize)
            );
            nextToSendLinearCombinationValue += packageSize;
        }
	}

	private void startGeneComparision() {
		for (Map.Entry<Integer,String> entry1 : this.genes.entrySet()) {
			Integer firstID = entry1.getKey();
			String firstRNA = entry1.getValue();

			for (Map.Entry<Integer, String> entry2 : this.genes.entrySet()) {
				Integer secondID = entry2.getKey();
				String secondRNA = entry2.getValue();

				if (firstID < secondID) {
					this.assign(new Worker.WorkMessageGeneComparision(firstRNA, secondRNA, firstID, secondID));
				}
			}
		}
	}
}
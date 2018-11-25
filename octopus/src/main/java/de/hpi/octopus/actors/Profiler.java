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
import scala.Int;

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
		private List<List<Integer>> updatedPartOfCurrentRow;
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class CompletionMessageGeneComparsion extends Profiler.CompletionMessage {
		private static final long serialVersionUID = -6823000111281007872L;
		private CompletionMessageGeneComparsion() {}
		protected status result;
		private int length;
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
	private final Map<Integer, Integer> longestGeneSubString = new HashMap<>();

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
		this.startGeneComparision(message.genes);
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
						this.calulateLinearCombination();
					}
				}
				else if (work instanceof Worker.WorkMessageLinearCombination){
					CompletionMessageLinearCombination completionMessage = (CompletionMessageLinearCombination) message;
					Worker.WorkMessageLinearCombination workMessage = (Worker.WorkMessageLinearCombination) work;
					this.report(completionMessage);
					this.updateCurrentRow(workMessage.getStartIndex(), workMessage.getEndIndex(), completionMessage.updatedPartOfCurrentRow);
					if (unassignedWork.isEmpty() && busyWorkers.isEmpty()){
						if (this.currentRowNumber < this.crackedPasswordsAsInteger.size()){
							this.startLinearCombinationForNewRow();
						}
						else {
							List<Integer> passwordsWithPositivePrefix = this.currentRow.get(this.currentRow.size()-1);
							for (Map.Entry<Integer,String> entry : this.plainTextPasswords.entrySet()){
								Integer id = entry.getKey();
								String plainTextPassword = entry.getValue();

								int index = passwordsWithPositivePrefix.indexOf(plainTextPassword);
								if (index < 0) {
									this.prefixes.put(id, -1);
								}
								else {
									this.prefixes.put(id, 1);
								}
							}
							this.log.info("FINAL PASSWORDS WITH PREFIX 1: " + Arrays.toString(passwordsWithPositivePrefix.toArray()));
						}
					}
				}
				else if (work instanceof Worker.WorkMessageGeneComparision){
					CompletionMessageGeneComparsion completionMessage = (CompletionMessageGeneComparsion) message;
					Worker.WorkMessageGeneComparision workMessage = (Worker.WorkMessageGeneComparision) work;
					this.report(completionMessage);

					if (unassignedWork.isEmpty() && busyWorkers.isEmpty()){

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

	private	void calulateLinearCombination(){
		int sum = 0;
		//this.crackedPasswordsAsInteger = new ArrayList<>(Arrays.asList(3,2,5));
		for (int i = 0; i < this.crackedPasswordsAsInteger.size(); i++){
			sum += crackedPasswordsAsInteger.get(i);
		}
		this.log.info("Sum of passwords: " + sum);

		this.currentRow = new ArrayList<>(Collections.nCopies(sum/2+1, null));

		this.currentRow.set(0, new ArrayList<>());
		ArrayList<Integer> firstValue = new ArrayList<>();
		firstValue.add(crackedPasswordsAsInteger.get(0));
		this.currentRow.set(crackedPasswordsAsInteger.get(0), firstValue);
		this.currentRowNumber = 1;

		this.startLinearCombinationForNewRow();
	}

	private void startLinearCombinationForNewRow(){
		List<List<Integer>> previousRow = new ArrayList<>(this.currentRow);

		this.log.info("Current Row: " + this.currentRowNumber);

		final int packageSize = 10000;
		for (int i = 0; i < previousRow.size(); i += packageSize){
			this.assign(new Worker.WorkMessageLinearCombination(previousRow, crackedPasswordsAsInteger.get(this.currentRowNumber), i, Math.min(i+packageSize, previousRow.size())));
		}

		this.currentRowNumber++;
	}

	private void updateCurrentRow(int startIndex, int endIndex, List<List<Integer>> updatedPartOfCurrentRow) {
		for (int i = startIndex; i < endIndex; i++){
			this.currentRow.set(i, updatedPartOfCurrentRow.get(i-startIndex));
		}
	}

	private void startGeneComparision(Map<Integer, String> genes) {
		for (Map.Entry<Integer,String> entry : genes.entrySet()) {
			Integer firstID = entry.getKey();
			String firstRNA = entry.getValue();

			for (Map.Entry<Integer, String> entry : genes.entrySet()) {
				Integer secondID = entry.getKey();
				String secondRNA = entry.getValue();

				if (firstID < secondID) {
					this.assign(new Worker.WorkMessageGeneComparision(firstRNA, secondRNA, firstID, secondID));
				}
			}
		}
	}
}
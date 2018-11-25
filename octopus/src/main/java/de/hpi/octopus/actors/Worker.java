package de.hpi.octopus.actors;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import de.hpi.octopus.OctopusMaster;
import de.hpi.octopus.actors.Profiler.CompletionMessage;
import de.hpi.octopus.actors.Profiler.RegistrationMessage;
import lombok.AllArgsConstructor;
import lombok.Data;

public class Worker extends AbstractActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "worker";

	public static Props props() {
		return Props.create(Worker.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////
	
	@SuppressWarnings("unused")
	public abstract static class WorkMessage implements Serializable {
		protected static final long serialVersionUID = -7643194361868862395L;
		private WorkMessage() {}
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class WorkMessagePasswordCracking extends WorkMessage {
		private static final long serialVersionUID = -7643194369068862395L;
		private WorkMessagePasswordCracking() {}
		private String[] sixDigitNumbers;
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class WorkMessageLinearCombination extends WorkMessage {
		private static final long serialVersionUID = -7643194361869062395L;
		private WorkMessageLinearCombination() {}
		private List<List<Integer>> previousRow;
		private int password;
		private int startIndex;
		private int endIndex;
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class WorkMessageGeneComparision extends WorkMessage {
		private static final long serialVersionUID = -7643190061869062395L;
		private WorkMessageGeneComparision() {}
		private String first;
		private String second;
		private int firstID;
		private int secondID;
	}

	/////////////////
	// Actor State //
	/////////////////
	
	private final LoggingAdapter log = Logging.getLogger(this.context().system(), this);
	private final Cluster cluster = Cluster.get(this.context().system());

	/////////////////////
	// Actor Lifecycle //
	/////////////////////
	
	@Override
	public void preStart() {
		this.cluster.subscribe(this.self(), MemberUp.class);
	}

	@Override
	public void postStop() {
		this.cluster.unsubscribe(this.self());
	}

	////////////////////
	// Actor Behavior //
	////////////////////
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(CurrentClusterState.class, this::handle)
				.match(MemberUp.class, this::handle)
				.match(WorkMessagePasswordCracking.class, this::handle)
				.match(WorkMessageLinearCombination.class, this::handle)
				.match(WorkMessageGeneComparision.class, this::handle)
				.matchAny(object -> this.log.info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(CurrentClusterState message) {
		message.getMembers().forEach(member -> {
			if (member.status().equals(MemberStatus.up()))
				this.register(member);
		});
	}

	private void handle(MemberUp message) {
		this.register(message.member());
	}

	private void register(Member member) {
		if (member.hasRole(OctopusMaster.MASTER_ROLE))
			this.getContext()
				.actorSelection(member.address() + "/user/" + Profiler.DEFAULT_NAME)
				.tell(new RegistrationMessage(), this.self());
	}

	private void handle(WorkMessagePasswordCracking message) {
		/*
		String[] numbers = new String[2];
		numbers[0] = "000900";
		numbers[1] = "000001";
		this.log.info("test: " + this.calculateHashes(numbers)[0] + " ###AND### " + this.calculateHashes(numbers)[1]);*/

		this.log.info("Recieved work package. Smallest password: " + message.sixDigitNumbers[0]);
		String[] hashes = this.calculateHashes(message.sixDigitNumbers);

		this.sender().tell(new Profiler.CompletionMessagePasswordCracking(CompletionMessage.status.SUCCESS, hashes), this.self());
	}

	private void handle(WorkMessageLinearCombination message) {
		/*
		String[] numbers = new String[2];
		numbers[0] = "000000";
		numbers[1] = "000004";
		this.log.info("test: " + this.calculateHashes(numbers)[0] + " ###AND### " + this.calculateHashes(numbers)[1]);
		*/

		this.log.info("Recieved work package. Smallest index: " + message.startIndex + ". Biggest Index: " + message.endIndex);
		List<List<Integer>> currentRow = this.calculateNewRow(message.previousRow, message.password, message.startIndex, message.endIndex);

		this.sender().tell(new Profiler.CompletionMessageLinearCombination(Profiler.CompletionMessageLinearCombination.status.SUCCESS, currentRow), this.self());
	}

	private void handle(WorkMessageGeneComparision message) {
		this.log.info("Recieved work package for gene comparision.");

		int length = 42;

		this.sender().tell(new Profiler.CompletionMessageGeneComparsion(Profiler.CompletionMessageGeneComparsion.status.SUCCESS, length), this.self());
	}
	
	private boolean isPrime(long n) {
		
		// Check for the most basic primes
		if (n == 1 || n == 2 || n == 3)
			return true;

		// Check if n is an even number
		if (n % 2 == 0)
			return false;

		// Check the odds
		for (long i = 3; i * i <= n; i += 2)
			if (n % i == 0)
				return false;
		
		return true;
	}

	private String calulateHash(String password) {
		MessageDigest digest = null;
		try {
			digest = MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		byte[] hashedBytes = new byte[0];
		try {
			hashedBytes = digest.digest(password.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		StringBuffer stringBuffer = new StringBuffer();
		for (int i = 0; i < hashedBytes.length; i++){
			stringBuffer.append(Integer.toString((hashedBytes[i] & 0xff) + 0x100, 16).substring(1));
		}
		return stringBuffer.toString();
	}

	private String[] calculateHashes(String[] sixDigitNumbers){
		String[] calculatedHashes = new String[sixDigitNumbers.length];
		for (int i = 0; i < sixDigitNumbers.length; i++){
			calculatedHashes[i] = this.calulateHash(sixDigitNumbers[i]);
		}
		return calculatedHashes;
	}

	private List<List<Integer>> calculateNewRow (List<List<Integer>> previousRow, int password, int startIndex, int endIndex){
		List<List<Integer>> currentRow = new ArrayList<>(Collections.nCopies(endIndex-startIndex, null));
		for(int i = startIndex; i < endIndex; i++){
			if(previousRow.get(i) == null){
				if( i-password >= 0 && previousRow.get(i-password) != null){
					List<Integer> temp = new ArrayList<>(previousRow.get(i-password));
					temp.add(password);
					currentRow.set(i-startIndex, temp);
				}
			}
			else{
				currentRow.set(i-startIndex, previousRow.get(i));
			}
		}
		return currentRow;
	}
}
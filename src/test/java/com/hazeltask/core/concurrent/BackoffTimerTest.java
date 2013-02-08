package com.hazeltask.core.concurrent;

import java.lang.ref.WeakReference;

import org.junit.Test;

import com.hazeltask.core.concurrent.BackoffTimer.BackoffTask;

//TODO: implement this
public class BackoffTimerTest {

	static class OneSecondTask extends BackoffTask {
		@Override
		public boolean execute() {
			try {
				System.out.println("+");
				Thread.sleep(1000);
				System.out.println("-");
			} catch (InterruptedException e) {
				System.out.println("onesecond task inturrupted...");
			}
			return true;
		}		
	}
	
	static class TenSecondTask extends BackoffTask {
		@Override
		public boolean execute() {
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				System.out.println("tensecond task inturrupted...");
			}
			return true;
		}		
	}
	
	@Test
	public void setupData() throws InterruptedException {
		BackoffTimer timer = new BackoffTimer("jason");
		
		OneSecondTask task = new OneSecondTask();
		OneSecondTask task2 = new OneSecondTask();
		
		System.out.println("schedule");
		timer.schedule(task, 1000, 1000);
		System.out.println("unschedule");
		timer.unschedule(task);
		System.out.println("unschedule");
		timer.unschedule(task2);
		System.out.println("done unschedule");
		timer.stop();
		Thread.currentThread().sleep(6000);
		System.out.println("new task...");
		timer.schedule(task2, 1000, 1000);
		
		//System.out.println((timer.get().isRunning()?"timer is running":"timer is stopped"));
		
		
		
		Thread.currentThread().join();
	}
}

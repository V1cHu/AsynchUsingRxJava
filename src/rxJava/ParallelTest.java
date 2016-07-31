package rxJava;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import rx.Observable;
import rx.Observer;
import rx.Subscriber;
import rx.schedulers.Schedulers;

public class ParallelTest {

	static int extractCount = 0;
	static int pushCount = 0;
	static int errorCount = 0;

	public static void main(String[] args) {

		Observable<String> resultsFromDB = getFromDB();

		resultsFromDB.subscribe(new Observer<String>() {

			@Override
			public void onCompleted() {
				System.out.println("DB retrieval completed.");
				printStatus();
			}

			@Override
			public void onError(Throwable e) {
				e.printStackTrace();
			}

			@Override
			public void onNext(String t) {
				publishToQueue(t).subscribe(new Observer<Boolean>() {

					@Override
					public void onCompleted() {
						System.out.println("Publish " + t + " complete.");

					}

					@Override
					public void onError(Throwable e) {

					}

					@Override
					public void onNext(Boolean b) {
						if (b) {
							System.out
									.println("Successfully pushed:" + t + " using " + Thread.currentThread().getName());
							pushCount++;
						} else {
							System.out.println("Failed to push:" + t + " using " + Thread.currentThread().getName());
							errorCount++;
						}
					}
				});
			}
		});

		// simulate realtime
		while (true) {
		}

	}

	private static Observable<String> getFromDB() {

		return Observable.create((Subscriber<? super String> s) -> {
			// simulate latency
			List<String> list = new ArrayList<String>();
			try {
				for (int i = 0; i < 100; i++) {
					list.add(Integer.toString(i));
				}
				Thread.sleep(700);
				System.out.println("DB op complete.");
			} catch (Exception e) {
				s.onError(e);
			}
			for (String st : list) {
				extractCount++;
				s.onNext(st);
			}
			s.onCompleted();
		}).subscribeOn(Schedulers.io());
	}

	protected static Observable<Boolean> publishToQueue(String t) {
		return Observable.defer(() -> {
			boolean b = false;
			try {
				if (t.contains("7")) {
					Thread.sleep(new Random().nextInt(9)+1*100);
				}
				System.out.println("Publishing:: " + t);
				b = true;

				if (Integer.parseInt(t) % 13 == 0) {
					b = false;
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			return Observable.just(b).subscribeOn(Schedulers.io());
		});
	}

	private static void printStatus() {
		System.out.println("----------------");
		System.out.println("Extracted: " + extractCount);
		System.out.println("Published: " + pushCount);
		System.out.println("Errored: " + errorCount);
		System.out.println("----------------");
	}

}

package rxJava;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import rx.Observable;
import rx.Observer;
import rx.functions.Action1;

public class TestClass {

	public static void main(String[] args) {

		TestClass test = new TestClass();
		System.out.println("---START---");

		test.getFromDB().subscribe(new Observer<String>() {

			@Override
			public void onCompleted() {
				System.out.println("Publish complete.");
			}

			@Override
			public void onError(Throwable t) {
				System.out.println(t.getMessage());
			}

			@Override
			public void onNext(String s) {
				test.publishToQueue(s).subscribe(new Observer<Boolean>() {

					@Override
					public void onNext(Boolean b) {
						if (b) {
							System.out.println("Successfully published.");
						}
					}

					@Override
					public void onCompleted() {
					}

					@Override
					public void onError(Throwable arg0) {
					}
				});
			};
		});
		System.out.println("---END---");
	}

	public Observable<String> getFromDB() {

		List<String> list = new ArrayList<String>();
		for (int i = 0; i < 30; i++) {
			list.add(Integer.toString(i));
		}
		return Observable.from(list).doOnNext(new Action1<String>() {
			@Override
			public void call(String temp) {
				// if (temp.contains("2")) {
				// try {
				// Thread.sleep(200);
				// } catch (InterruptedException e) {
				// e.printStackTrace();
				// }
				// }
			}
		}).delay(200, TimeUnit.MILLISECONDS);

	}

	public Observable<Boolean> publishToQueue(String s) {

		return Observable.defer(() -> {
			// try {
			// if (s.contains("7")) {
			// Thread.sleep(700);
			// }
			// System.out.println("Published:: " + s);
			// } catch (InterruptedException e) {
			// e.printStackTrace();
			// }
			return Observable.just(true).delay(500, TimeUnit.MILLISECONDS);
		});
	}
}

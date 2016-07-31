package rxJava;

import java.util.ArrayList;
import java.util.List;

import rx.Observable;
import rx.Observer;
import rx.schedulers.Schedulers;

public class ParallelTestNew {

	public static void main(String[] args) {

		ParallelTestNew test = new ParallelTestNew();
		System.out.println("---START---");

		test.getFromDB().subscribeOn(Schedulers.io()).map((s) -> {
			System.out.println("Got :" + s);
			return s + "-mod";
		}).observeOn(Schedulers.computation()).flatMap(s -> test.publishToQueue(s)).subscribe(new Observer<Boolean>() {

			@Override
			public void onCompleted() {
				System.out.println("Completed");
			}

			@Override
			public void onError(Throwable e) {
				e.printStackTrace();
			}

			@Override
			public void onNext(Boolean t) {
				System.out.println("Published");
			}
		});
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("---END---");
	}

	public Observable<String> getFromDB() {

		List<String> list = new ArrayList<String>();
		for (int i = 0; i < 5; i++) {
			list.add(Integer.toString(i));
		}

		System.out.println("DB retrieval complete.");
		return Observable.from(list);
	}

	public Observable<Boolean> publishToQueue(String l) {

		return Observable.defer(() -> {
			System.out.println(l);

			return Observable.just(true);
		});
	}
}

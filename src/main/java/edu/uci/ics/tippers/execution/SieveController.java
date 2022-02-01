package edu.uci.ics.tippers.execution;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpStatus;
import edu.uci.ics.tippers.execution.MiddleWare.mget_obj;
import edu.uci.ics.tippers.dbms.MallData;
import edu.uci.ics.tippers.execution.MiddleWare.Message;
import edu.uci.ics.tippers.execution.MiddleWare.ops;
@RestController
public class SieveController {

	private static final String template = "Hello, %s!";
	private final AtomicLong counter = new AtomicLong();

	@PostMapping(value = "/mget_obj", consumes = "application/json", produces = "application/json")
	public ResponseEntity<Message> mget(@RequestBody mget_obj getobj) {
		System.out.println(getobj.getId()[0]);
		System.out.println(getobj);
		System.out.println(getobj.getProp()[0].getProp());
		System.out.println(getobj.getProp()[0].getInfo());
		String querier = getobj.getId()[0];
		String prop = getobj.getProp()[0].getProp();
		String info = getobj.getProp()[0].getInfo();
		ops op = new ops();
		MallData[] res = op.get(querier, prop, info);
		Message msg = new Message("Succ", res);
		return new ResponseEntity<>(msg, HttpStatus.OK);
	}
}

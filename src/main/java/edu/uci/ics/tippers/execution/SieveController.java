package edu.uci.ics.tippers.execution;

import org.springframework.web.bind.annotation.DeleteMapping;
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
	@GetMapping(value = "/mget_entry", produces = "application/json")
	public ResponseEntity<Message> mget_entry(@RequestParam(value="key") String key) {
		ops op = new ops();
		MallData[] res = op.getpersonalData(key);
		if (res == null) {
			Message msg = new Message("Fail, data not found", null);
			return new ResponseEntity<>(msg, HttpStatus.NOT_FOUND);
		}
		Message msg = new Message("Succ", res);
		return new ResponseEntity<>(msg, HttpStatus.OK);
	}

	@PostMapping(value = "/mget_obj", consumes = "application/json", produces = "application/json")
	public ResponseEntity<Message> mget(@RequestBody mget_obj getobj) {
		String querier = getobj.getId()[0];
		String prop = getobj.getProp()[0].getProp();
		String info = getobj.getProp()[0].getInfo();
		ops op = new ops();
		MallData[] res = op.get(querier, prop, info);
		if (res == null) {
			Message msg = new Message("Fail, data not found", null);
			return new ResponseEntity<>(msg, HttpStatus.NOT_FOUND);
		}
		Message msg = new Message("Succ", res);
		return new ResponseEntity<>(msg, HttpStatus.OK);
	}
	@PostMapping(value = "/mdelete_UserMetaobj", consumes = "application/json", produces = "application/json")
	public ResponseEntity<Message> mdelete_UserMetaObj(@RequestBody mget_obj getobj) {
		String querier = getobj.getId()[0];
		String prop = getobj.getProp()[0].getProp();
		String info = getobj.getProp()[0].getInfo();
		ops op = new ops();
		Integer Status = op.delete(querier, prop, info);
		if (Status == 200) {
			Message msg = new Message("Succ", null);
			return new ResponseEntity<>(msg, HttpStatus.OK);
		}
		Message msg = new Message("Fail, bad entry", null);
		return new ResponseEntity<>(msg, HttpStatus.BAD_REQUEST);
		
	}

	@DeleteMapping(value = "/mdelete_obj", produces = "application/json")
	public ResponseEntity<Message> mdelete_obj(@RequestParam(value="key") String key){
		ops op = new ops();
		int status = op.deletePersonalData(key);
		if (status == 0) {
			Message msg = new Message("Succ", null);
			return new ResponseEntity<>(msg, HttpStatus.OK);
		}
		Message msg = new Message("Fail, bad entry", null);
		return new ResponseEntity<>(msg, HttpStatus.BAD_REQUEST);
		// SELECT UNIQUE policy_id from user_policy_object_condition WHERE attribute = device_id AND comp_value = ""
		// DELETE from user_policy where id = ...
		// DELETE from user_policy_object_condition where policy id = ...
		// DELETE from mall_observation where device_id = device_id

	}
}

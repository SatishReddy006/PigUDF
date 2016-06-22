package com.transformation.udf;

import java.util.UUID;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

public class GetHash extends EvalFunc<String> {

	public String exec(Tuple input) throws ExecException {

		String str = input.get(0).toString();
		if (str.equals("~")) {
			UUID u = UUID.randomUUID();
			str = u.toString();
		}
		str = new StringBuilder().append(str).reverse().toString();
		long h = 1125899906842597L;
		int len = str.length();

		for (int i = 0; i < len; i++) {
			h = 31 * h + str.charAt(i);
		}
		str = null;
		return String.valueOf(h);
	}

}
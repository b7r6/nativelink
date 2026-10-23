const assert = require("node:assert/strict");
const { once } = require("node:events");
const http = require("node:http");
const { createRequire } = require("node:module");
const { test } = require("node:test");

for (const consumer of ["@azure/ms-rest-js", "sequelize"]) {
	test(`${consumer} can use the patched CommonJS UUID dependency`, () => {
		const consumerRequire = createRequire(
			require.resolve(`${consumer}/package.json`),
		);
		const uuid = consumerRequire("uuid");
		assert.equal(uuid.version(uuid.v1()), 1);
		assert.equal(uuid.version(uuid.v4()), 4);

		// GHSA-w5hq-g745-h8pq: reject a short buffer without partially writing it.
		for (const generate of [uuid.v3, uuid.v5]) {
			const buffer = new Uint8Array(8).fill(0xaa);
			assert.throws(() => generate("test", uuid.v5.DNS, buffer, 4), RangeError);
			assert.deepEqual(buffer, new Uint8Array(8).fill(0xaa));
		}
	});
}

test("the Azure SDK and Sequelize still generate UUIDs through their public APIs", () => {
	const sdk = require("@azure/ms-rest-js");
	const { DataTypes, Utils } = require("sequelize");
	const uuid = require("uuid");
	assert.equal(uuid.version(sdk.generateUuid()), 4);
	assert.equal(uuid.version(Utils.toDefaultValue(new DataTypes.UUIDV1())), 1);
	assert.equal(uuid.version(Utils.toDefaultValue(new DataTypes.UUIDV4())), 4);
});

test("the telemetry SDK bounds incoming baggage entries", () => {
	const telemetryRequire = createRequire(
		require.resolve("applicationinsights/package.json"),
	);
	const { W3CBaggagePropagator } = telemetryRequire("@opentelemetry/core");
	const { ROOT_CONTEXT, defaultTextMapGetter, propagation } =
		telemetryRequire("@opentelemetry/api");
	const baggage = Array.from({ length: 200 }, (_, i) => `k${i}=v`).join(",");
	const context = new W3CBaggagePropagator().extract(
		ROOT_CONTEXT,
		{ baggage },
		defaultTextMapGetter,
	);
	assert.equal(propagation.getBaggage(context).getAllEntries().length, 180);
});

// Exercise the copy used by Azurite's axios dependency, across two local origins.
async function headersAfterRedirect(headers, sensitiveHeaders = []) {
	const axiosRequire = createRequire(require.resolve("axios/package.json"));
	const { http: followHttp } = axiosRequire("follow-redirects");
	let originHeaders;
	let targetHeaders;
	let request;
	const target = http.createServer((req, res) => {
		targetHeaders = req.headers;
		res.end("ok");
	});
	const origin = http.createServer((req, res) => {
		originHeaders = req.headers;
		res.writeHead(302, {
			location: `http://127.0.0.1:${target.address().port}/`,
		});
		res.end();
	});

	try {
		target.listen(0, "127.0.0.1");
		await once(target, "listening");
		origin.listen(0, "127.0.0.1");
		await once(origin, "listening");
		const host = `127.0.0.1:${origin.address().port}`;
		request = followHttp.request(`http://${host}/`, {
			// Node does not add Host to raw array headers. Without it the old
			// library gets HTTP 400 before the redirect, masking the actual leak.
			headers: Array.isArray(headers)
				? ["Host", host, ...headers]
				: { Host: host, ...headers },
			sensitiveHeaders,
		});
		request.setTimeout(5000, () =>
			request.destroy(new Error("redirect timed out")),
		);
		const response = once(request, "response");
		request.end();
		const [res] = await response;
		res.resume();
		await once(res, "end");
		assert.equal(res.statusCode, 200);
		assert.ok(originHeaders, "request must reach the redirecting origin");
		assert.ok(targetHeaders, "request must reach the redirect target");
		return { originHeaders, targetHeaders };
	} finally {
		request?.destroy();
		await Promise.all(
			[origin, target].map(
				(server) => new Promise((resolve) => server.close(resolve)),
			),
		);
	}
}

test("follow-redirects strips array-form credentials across origins", async () => {
	// GHSA-q3jh-wrc6-jgq8: raw arrays bypassed the credential header filter.
	const credentials = {
		Authorization: "Bearer secret",
		Cookie: "session=secret",
		"Proxy-Authorization": "Basic secret",
	};
	const { originHeaders, targetHeaders } = await headersAfterRedirect([
		...Object.entries(credentials).flat(),
		"X-Trace",
		"kept",
	]);
	for (const [name, value] of Object.entries(credentials)) {
		assert.equal(originHeaders[name.toLowerCase()], value);
		assert.equal(
			targetHeaders[name.toLowerCase()],
			undefined,
			`${name} leaked across origins`,
		);
	}
	assert.equal(targetHeaders["x-trace"], "kept");
});

for (const name of ["X-Api^Key", "X-Token|Secret"]) {
	test(`follow-redirects strips the declared sensitive header ${name}`, async () => {
		// GHSA-8r9p-f939-6h3c: both legal header-name metacharacters need escaping.
		const { originHeaders, targetHeaders } = await headersAfterRedirect(
			{ [name]: "secret", "X-Trace": "kept" },
			[name],
		);
		assert.equal(originHeaders[name.toLowerCase()], "secret");
		assert.equal(
			targetHeaders[name.toLowerCase()],
			undefined,
			`${name} leaked across origins`,
		);
		assert.equal(targetHeaders["x-trace"], "kept");
	});
}

import { expect, test } from "bun:test";
import { createRequire } from "node:module";
import { JSDOM } from "jsdom";

// Mermaid renders the docs diagrams; check the sanitizer that it resolves.
const mermaidRequire = createRequire(require.resolve("mermaid"));
const createDOMPurify = mermaidRequire("dompurify") as typeof import("dompurify").default;

test("DOMPurify removes SVG href animation in XHTML mode", () => {
  // GHSA-c6gr-qvv4-5v3f: XHTML preserves the case of attributeName.
  const { window } = new JSDOM("");
  try {
    const purify = createDOMPurify(window);
    const clean = purify.sanitize(
      '<svg xmlns="http://www.w3.org/2000/svg"><a><animate attributeName="href" ' +
        'values="x;javascript:alert(1)" dur="0.01s" fill="freeze"/>' +
        "<text>safe label</text></a></svg>",
      {
        PARSER_MEDIA_TYPE: "application/xhtml+xml",
        ALLOWED_TAGS: ["svg", "a", "animate", "text"],
        ALLOWED_ATTR: ["attributeName", "values", "dur", "fill"],
      },
    );
    const doc = new window.DOMParser().parseFromString(clean, "application/xhtml+xml");
    const animation = doc.querySelector("animate");
    expect(animation).not.toBeNull();
    expect(animation?.hasAttribute("attributeName")).toBe(false);
    expect(doc.querySelector("text")?.textContent).toBe("safe label");
  } finally {
    window.close();
  }
});

test("DOMPurify scrubs template expressions assembled inside a shadow root", () => {
  // GHSA-cg2g-xh64-v4q7: removing a separator joins individually safe fragments.
  const { window } = new JSDOM("");
  try {
    const purify = createDOMPurify(window);
    const doc = window.document;
    const host = doc.createElement("div");
    const shadow = host.attachShadow({ mode: "open" });
    const label = doc.createElement("span");
    label.textContent = "safe label";
    shadow.append(
      doc.createTextNode("$"),
      doc.createElement("object"),
      doc.createTextNode("{alert(document.domain)}"),
      label,
    );

    purify.sanitize(host, { IN_PLACE: true, SAFE_FOR_TEMPLATES: true });
    shadow.normalize();
    expect(shadow.textContent).not.toContain("${alert(document.domain)}");
    expect(shadow.querySelector("object")).toBeNull();
    expect(shadow.querySelector("span")?.textContent).toBe("safe label");
  } finally {
    window.close();
  }
});

for (const mode of ["parent", "nextSibling"] as const) {
  test(`DOMPurify neutralizes event handlers when a hook detaches the ${mode}`, () => {
    // GHSA-mrv2-3pqr-j5jr: detached elements must be cleaned as well as the returned tree.
    const { window } = new JSDOM("");
    try {
      const purify = createDOMPurify(window);
      const doc = window.document;
      doc.body.innerHTML =
        '<div id="root"><figure><img class="bad" src="data:,">' +
        '<img id="evil" src="data:," onerror="alert(1)"></figure><p>safe label</p></div>';
      const root = doc.getElementById("root");
      const evil = doc.getElementById("evil");
      if (!root || !evil) throw new Error("missing DOM fixture");
      expect(evil.getAttribute("onerror")).toBe("alert(1)");
      purify.addHook("afterSanitizeAttributes", (node) => {
        if (node.getAttribute("class") !== "bad") return;
        if (mode === "parent") node.parentElement?.remove();
        else node.nextElementSibling?.remove();
      });

      purify.sanitize(root, { IN_PLACE: true });
      expect(evil.isConnected).toBe(false);
      expect(evil.hasAttribute("onerror")).toBe(false);
      expect(root.querySelector("p")?.textContent).toBe("safe label");
    } finally {
      window.close();
    }
  });
}

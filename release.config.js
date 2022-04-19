module.exports = {
  branches: ["main"],
  tagFormat: "${version}",
  plugins: [
    [
      "@semantic-release/commit-analyzer",
      {
        releaseRules: [
          { breaking: true, release: "major" },
          { type: "docs", release: "patch" },
          { type: "chore", release: "patch" },
          { type: "test", release: "patch" },
          { type: "fix", release: "patch" },
          { type: "feat", subject: "*BREAKING*", release: "major" },
          { type: "feat", scope: "BREAKING", release: "major" },
          { type: "feat", release: "minor" },
        ],
        parserOpts: {
          noteKeywords: ["BREAKING CHANGE", "BREAKING CHANGES", "BREAKING"],
        },
      },
    ],
    "@semantic-release/release-notes-generator",
    [
      "@semantic-release/npm",
      {
        npmPublish: true,
      },
    ],
    "@semantic-release/gitlab",
    [
      "@semantic-release/git",
      {
        assets: ["CHANGELOG.md", "package.json"],
      },
    ],
  ],
};

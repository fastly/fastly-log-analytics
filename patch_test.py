def _patch():
    with open("frontend/__tests__/components/security/ProxyWatchdogSection.test.tsx") as f:
        content = f.read()

    content = content.replace(
        "const mockData: components['schemas']['SecurityProxiesResponse'] = {", "const mockData = {"
    )
    content = content.replace("data={mockData}", "data={mockData as any}")

    with open("frontend/__tests__/components/security/ProxyWatchdogSection.test.tsx", "w") as f:
        f.write(content)


_patch()

def _patch():
    with open("frontend/app/security/_sections/ProxyWatchdogSection.tsx") as f:
        content = f.read()

    content = content.replace(
        "type SecurityProxiesResponse = components['schemas']['SecurityProxiesResponse']",
        "type SecurityProxiesResponse = components['schemas']['SecurityAggregatesResponse']",
    )
    content = content.replace("(item) => (", "(item: any) => (")
    content = content.replace("row.original as any", "row.original")

    with open("frontend/app/security/_sections/ProxyWatchdogSection.tsx", "w") as f:
        f.write(content)


_patch()

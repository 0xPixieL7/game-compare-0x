import { Head, Link } from '@inertiajs/react';

export default function Welcome() {
    return (
        <>
            <Head title="Fast Resume + Cover Letter Delivery">
                <link rel="preconnect" href="https://fonts.bunny.net" />
                <link
                    href="https://fonts.bunny.net/css?family=inter:400,500,600,700,800&display=swap"
                    rel="stylesheet"
                />
            </Head>

            <div className="min-h-screen bg-[#07070a] text-white">
                <header className="mx-auto flex w-full max-w-6xl items-center justify-between px-6 py-6">
                    <div className="text-lg font-semibold tracking-tight">
                        Rapid Resume
                    </div>
                    <nav className="flex items-center gap-6 text-sm text-white/70">
                        <a href="#pricing" className="hover:text-white">
                            Pricing
                        </a>
                        <a href="#process" className="hover:text-white">
                            How it works
                        </a>
                        <a href="#contact" className="hover:text-white">
                            Contact
                        </a>
                    </nav>
                </header>

                <main className="mx-auto w-full max-w-6xl px-6 pb-20">
                    <section className="grid gap-10 pt-12 lg:grid-cols-[1.1fr_0.9fr] lg:items-center">
                        <div className="space-y-6">
                            <p className="inline-flex items-center gap-2 rounded-full border border-white/10 bg-white/5 px-4 py-1 text-xs uppercase tracking-[0.2em] text-white/70">
                                24‑hour or rush delivery
                            </p>
                            <h1 className="text-4xl font-black leading-tight sm:text-5xl">
                                Get a job‑winning resume + cover letter — fast.
                            </h1>
                            <p className="text-lg text-white/70">
                                ATS‑friendly rewrite, clean formatting, and a cover letter tailored
                                to your target role. Choose your turnaround and get it done today.
                            </p>
                            <div className="flex flex-wrap gap-4">
                                <a
                                    href="#pricing"
                                    className="rounded-full bg-blue-500 px-6 py-3 text-sm font-semibold text-white shadow-lg shadow-blue-500/30"
                                >
                                    See pricing
                                </a>
                                <a
                                    href="#contact"
                                    className="rounded-full border border-white/20 px-6 py-3 text-sm font-semibold text-white/80"
                                >
                                    Get started
                                </a>
                            </div>
                            <div className="flex flex-wrap gap-4 text-xs text-white/60">
                                <span>✔ ATS‑friendly</span>
                                <span>✔ 1 revision</span>
                                <span>✔ Clear, modern layout</span>
                            </div>
                        </div>
                        <div className="rounded-3xl border border-white/10 bg-white/5 p-8 shadow-2xl">
                            <h2 className="text-xl font-semibold">What you get</h2>
                            <ul className="mt-4 space-y-3 text-sm text-white/70">
                                <li>• Resume rewrite optimized for clarity + impact</li>
                                <li>• Tailored cover letter for your target role</li>
                                <li>• Clean, professional formatting</li>
                                <li>• One revision included</li>
                            </ul>
                            <p className="mt-6 text-xs text-white/50">
                                Send your current resume + target job link to begin.
                            </p>
                        </div>
                    </section>

                    <section
                        id="pricing"
                        className="mt-16 rounded-3xl border border-white/10 bg-black/40 p-8"
                    >
                        <h2 className="text-2xl font-bold">Pricing (time‑based)</h2>
                        <div className="mt-6 grid gap-6 md:grid-cols-2">
                            <div className="rounded-2xl border border-white/10 bg-white/5 p-6">
                                <p className="text-sm text-white/60">24‑hour delivery</p>
                                <p className="mt-2 text-3xl font-bold">$10</p>
                            </div>
                            <div className="rounded-2xl border border-white/10 bg-white/5 p-6">
                                <p className="text-sm text-white/60">6‑hour delivery</p>
                                <p className="mt-2 text-3xl font-bold">$20</p>
                            </div>
                            <div className="rounded-2xl border border-white/10 bg-white/5 p-6">
                                <p className="text-sm text-white/60">2‑hour delivery</p>
                                <p className="mt-2 text-3xl font-bold">$30</p>
                            </div>
                            <div className="rounded-2xl border border-white/10 bg-white/5 p-6">
                                <p className="text-sm text-white/60">1‑hour rush</p>
                                <p className="mt-2 text-3xl font-bold">$40</p>
                            </div>
                        </div>
                        <p className="mt-4 text-xs text-white/50">
                            Payment via PayPal: <span className="text-white">femiodunaiya@gmail.com</span>
                        </p>
                    </section>

                    <section id="process" className="mt-16">
                        <h2 className="text-2xl font-bold">How it works</h2>
                        <div className="mt-6 grid gap-6 md:grid-cols-3">
                            <div className="rounded-2xl border border-white/10 bg-white/5 p-6">
                                <p className="text-sm text-white/60">Step 1</p>
                                <p className="mt-2 text-base font-semibold">Send your resume</p>
                                <p className="mt-2 text-sm text-white/70">
                                    Email your current resume + the job link you want.
                                </p>
                            </div>
                            <div className="rounded-2xl border border-white/10 bg-white/5 p-6">
                                <p className="text-sm text-white/60">Step 2</p>
                                <p className="mt-2 text-base font-semibold">We rewrite + format</p>
                                <p className="mt-2 text-sm text-white/70">
                                    ATS‑friendly structure, stronger bullets, clean design.
                                </p>
                            </div>
                            <div className="rounded-2xl border border-white/10 bg-white/5 p-6">
                                <p className="text-sm text-white/60">Step 3</p>
                                <p className="mt-2 text-base font-semibold">Get your files</p>
                                <p className="mt-2 text-sm text-white/70">
                                    Receive your resume + cover letter on time.
                                </p>
                            </div>
                        </div>
                    </section>

                    <section id="contact" className="mt-16 rounded-3xl border border-white/10 bg-white/5 p-8">
                        <h2 className="text-2xl font-bold">Get started</h2>
                        <p className="mt-3 text-sm text-white/70">
                            Send your resume + target job link and your desired turnaround.
                        </p>
                        <div className="mt-6 space-y-2 text-sm">
                            <p>
                                Email: <a className="text-blue-300" href="mailto:femiodunaiya@gmail.com">femiodunaiya@gmail.com</a>
                            </p>
                            <p>
                                Twitter: <a className="text-blue-300" href="https://twitter.com/monsieurlowkey">@monsieurlowkey</a>
                            </p>
                        </div>
                        <p className="mt-4 text-xs text-white/50">
                            One revision included. Rush delivery starts once payment is confirmed.
                        </p>
                    </section>
                </main>

                <footer className="border-t border-white/10 py-8 text-center text-xs text-white/40">
                    © {new Date().getFullYear()} Rapid Resume. All rights reserved.
                </footer>
            </div>
        </>
    );
}

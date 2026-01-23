import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import AppLayout from '@/layouts/app-layout';
import { Head } from '@inertiajs/react';

export default function TermsOfService() {
    return (
        <AppLayout
            breadcrumbs={[
                { title: 'Terms of Service', href: '/terms-of-service' },
            ]}
        >
            <Head title="Terms of Service" />
            <div className="flex h-full flex-col p-4 md:p-6 lg:p-8">
                <Card className="mx-auto w-full max-w-4xl border-none shadow-none">
                    <CardHeader className="px-0">
                        <CardTitle className="text-3xl font-bold">
                            Terms of Service
                        </CardTitle>
                        <p className="mt-2 text-muted-foreground">
                            Last updated: {new Date().toLocaleDateString()}
                        </p>
                    </CardHeader>
                    <CardContent className="space-y-6 px-0 leading-7 text-foreground/90">
                        <section>
                            <h2 className="mb-3 text-xl font-semibold text-foreground">
                                1. Acceptance of Terms
                            </h2>
                            <p>
                                By accessing or using Game Compare
                                ("Application"), you agree to be bound by these
                                Terms of Service ("Terms"). If you do not agree
                                to these Terms, please do not use the
                                Application.
                            </p>
                        </section>

                        <section>
                            <h2 className="mb-3 text-xl font-semibold text-foreground">
                                2. Description of Service
                            </h2>
                            <p>
                                Game Compare provides a platform for comparing
                                video game prices, viewing game details, and
                                tracking game data ("Service"). We strive to
                                provide accurate information, but we cannot
                                guarantee that all data is always up-to-date or
                                error-free.
                            </p>
                        </section>

                        <section>
                            <h2 className="mb-3 text-xl font-semibold text-foreground">
                                3. User Accounts
                            </h2>
                            <p>
                                To access certain features, you may need to
                                create an account. You are responsible for
                                maintaining the confidentiality of your account
                                credentials and for all activities that occur
                                under your account. You agree to notify us
                                immediately of any unauthorized use of your
                                account.
                            </p>
                        </section>

                        <section>
                            <h2 className="mb-3 text-xl font-semibold text-foreground">
                                4. User Conduct
                            </h2>
                            <p className="mb-2">
                                You agree not to use the Application to:
                            </p>
                            <ul className="list-disc space-y-1 pl-6">
                                <li>
                                    Violate any applicable local, state,
                                    national, or international law.
                                </li>
                                <li>Harass, abuse, or harm another person.</li>
                                <li>Impersonate any person or entity.</li>
                                <li>
                                    Interfere with or disrupt the Application or
                                    servers/networks connected to the
                                    Application.
                                </li>
                                <li>
                                    Attempt to gain unauthorized access to any
                                    portion of the Application.
                                </li>
                            </ul>
                        </section>

                        <section>
                            <h2 className="mb-3 text-xl font-semibold text-foreground">
                                5. Intellectual Property
                            </h2>
                            <p>
                                All content included on the Application, such as
                                text, graphics, logos, images, as well as the
                                compilation thereof, is the property of Game
                                Compare or its suppliers and protected by
                                copyright and other laws.
                            </p>
                        </section>

                        <section>
                            <h2 className="mb-3 text-xl font-semibold text-foreground">
                                6. Third-Party Links and Services
                            </h2>
                            <p>
                                Our application may contain links to third-party
                                web sites or services that are not owned or
                                controlled by Game Compare. We have no control
                                over, and assume no responsibility for, the
                                content, privacy policies, or practices of any
                                third-party web sites or services. You
                                acknowledge and agree that Game Compare shall
                                not be responsible or liable for any damage or
                                loss caused by or in connection with the use of
                                such content, goods or services available on or
                                through any such web sites or services.
                            </p>
                        </section>

                        <section>
                            <h2 className="mb-3 text-xl font-semibold text-foreground">
                                7. Epic Games Store
                            </h2>
                            <p>
                                If you access our Application via the Epic Games
                                Store, additional terms may apply as governed by
                                your agreement with Epic Games. We are not
                                responsible for the availability or performance
                                of the Epic Games Store service.
                            </p>
                        </section>

                        <section>
                            <h2 className="mb-3 text-xl font-semibold text-foreground">
                                8. Termination
                            </h2>
                            <p>
                                We may terminate or suspend your access to our
                                Service immediately, without prior notice or
                                liability, for any reason whatsoever, including
                                without limitation if you breach the Terms.
                            </p>
                        </section>

                        <section>
                            <h2 className="mb-3 text-xl font-semibold text-foreground">
                                9. Changes to Terms
                            </h2>
                            <p>
                                We reserve the right, at our sole discretion, to
                                modify or replace these Terms at any time. What
                                constitutes a material change will be determined
                                at our sole discretion. By continuing to access
                                or use our Service after those revisions become
                                effective, you agree to be bound by the revised
                                terms.
                            </p>
                        </section>

                        <section>
                            <h2 className="mb-3 text-xl font-semibold text-foreground">
                                10. Contact Us
                            </h2>
                            <p>
                                If you have any questions about these Terms,
                                please contact us at:{' '}
                                <a
                                    href="mailto:support@gamecompare.com"
                                    className="text-primary hover:underline"
                                >
                                    support@gamecompare.com
                                </a>
                                .
                            </p>
                        </section>
                    </CardContent>
                </Card>
            </div>
        </AppLayout>
    );
}

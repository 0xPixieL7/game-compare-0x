import { Head } from '@inertiajs/react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import AppLayout from '@/layouts/app-layout';

export default function PrivacyPolicy() {
    return (
        <AppLayout breadcrumbs={[{ title: 'Privacy Policy', href: '/privacy-policy' }]}>
            <Head title="Privacy Policy" />
            <div className="flex h-full flex-col p-4 md:p-6 lg:p-8">
                <Card className="mx-auto w-full max-w-4xl border-none shadow-none">
                    <CardHeader className="px-0">
                        <CardTitle className="text-3xl font-bold">Privacy Policy</CardTitle>
                        <p className="text-muted-foreground mt-2">Last updated: {new Date().toLocaleDateString()}</p>
                    </CardHeader>
                    <CardContent className="space-y-6 px-0 leading-7 text-foreground/90">
                        <section>
                            <h2 className="text-xl font-semibold mb-3 text-foreground">1. Introduction</h2>
                            <p>
                                Welcome to Game Compare ("Application"). We respect your privacy and are committed to protecting it through our compliance with this policy.
                                This policy describes the types of information we may collect from you or that you may provide when you visit the Application and our practices for collecting, using, maintaining, protecting, and disclosing that information.
                            </p>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3 text-foreground">2. Information We Collect</h2>
                            <p className="mb-2">We collect several types of information from and about users of our Application, including:</p>
                            <ul className="list-disc pl-6 space-y-1">
                                <li><strong>Personal Information:</strong> Name, email address, and other contact information when you register or sign in using Epic Games or other third-party providers.</li>
                                <li><strong>Usage Data:</strong> Information about your internet connection, the equipment you use to access our Application, and usage details.</li>
                            </ul>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3 text-foreground">3. How We Use Your Information</h2>
                            <p className="mb-2">We use information that we collect about you or that you provide to us, including any personal information:</p>
                            <ul className="list-disc pl-6 space-y-1">
                                <li>To present our Application and its contents to you.</li>
                                <li>To provide you with information, products, or services that you request from us.</li>
                                <li>To fulfill any other purpose for which you provide it.</li>
                                <li>To notify you about changes to our Application or any products or services we offer or provide through it.</li>
                                <li>To allow you to participate in interactive features on our Application.</li>
                            </ul>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3 text-foreground">4. Disclosure of Your Information</h2>
                            <p>
                                We do not sell, trade, or otherwise transfer to outside parties your Personally Identifiable Information unless we provide you with advance notice. This does not include website hosting partners and other parties who assist us in operating our website, conducting our business, or serving our users, so long as those parties agree to keep this information confidential.
                            </p>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3 text-foreground">5. Data Security</h2>
                            <p>
                                We have implemented measures designed to secure your personal information from accidental loss and from unauthorized access, use, alteration, and disclosure. All information you provide to us is stored on our secure servers behind firewalls.
                            </p>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3 text-foreground">6. Epic Games Store Requirements</h2>
                            <p>
                                If you access our Application via the Epic Games Store, we comply with relevant Epic Games privacy requirements, including obtaining your explicit consent for data access. We only access data (such as profile, friends list, or presence) that you explicitly authorize. We respect your privacy choices and do not process data in ways that contradict your permissions.
                            </p>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3 text-foreground">7. Children Under the Age of 13</h2>
                            <p>
                                Our Application is not intended for children under 13 years of age. No one under age 13 may provide any personal information to or on the Application. We do not knowingly collect personal information from children under 13.
                            </p>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3 text-foreground">8. Contact Information</h2>
                            <p>
                                To ask questions or comment about this privacy policy and our privacy practices, contact us at: <a href="mailto:support@gamecompare.com" className="text-primary hover:underline">support@gamecompare.com</a>.
                            </p>
                        </section>
                    </CardContent>
                </Card>
            </div>
        </AppLayout>
    );
}
